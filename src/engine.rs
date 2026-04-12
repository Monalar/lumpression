use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write, BufWriter};
use sha2::{Sha256, Digest};

pub struct LumpiEngine {
    schema_dict: HashMap<String, u16>,
    next_key_id: u16,
    value_stream: Vec<u8>,
}

impl LumpiEngine {
    pub fn new() -> Self {
        LumpiEngine {
            schema_dict: HashMap::new(),
            next_key_id: 0,
            value_stream: Vec::with_capacity(20 * 1024 * 1024), 
        }
    }

    pub fn compress(&mut self, input_path: &str, output_path: &str) -> io::Result<String> {
        let mut file = File::open(input_path)?;
        let mut raw_data = Vec::new();
        file.read_to_end(&mut raw_data)?;
        
        let mut hasher = Sha256::new();
        hasher.update(&raw_data);
        let hash_result = hex::encode(hasher.finalize());

        let len = raw_data.len();
        let mut cursor = 0;

        while cursor < len {
            while cursor < len && raw_data[cursor] != b'{' { cursor += 1; }
            if cursor >= len { break; }
            cursor += 1;

            loop {
                while cursor < len && raw_data[cursor] != b'"' && raw_data[cursor] != b'}' { cursor += 1; }
                if cursor >= len || raw_data[cursor] == b'}' { break; }
                cursor += 1; 
                
                let key_start = cursor;
                while cursor < len && raw_data[cursor] != b'"' { cursor += 1; }
                let key_slice = &raw_data[key_start..cursor];
                cursor += 1; 

                let key_str = unsafe { std::str::from_utf8_unchecked(key_slice) };
                let key_id = *self.schema_dict.entry(key_str.to_string()).or_insert_with(|| {
                    let id = self.next_key_id;
                    self.next_key_id += 1;
                    id
                });

                while cursor < len && raw_data[cursor] != b':' { cursor += 1; }
                cursor += 1;
                while cursor < len && raw_data[cursor].is_ascii_whitespace() { cursor += 1; }

                let is_string_type = if cursor < len && raw_data[cursor] == b'"' {
                    cursor += 1; true
                } else { false };

                let val_start = cursor;
                if is_string_type {
                    while cursor < len && raw_data[cursor] != b'"' { cursor += 1; }
                } else {
                    while cursor < len && raw_data[cursor] != b',' && raw_data[cursor] != b'}' && !raw_data[cursor].is_ascii_whitespace() {
                        cursor += 1;
                    }
                }
                let val_end = cursor;
                if is_string_type { cursor += 1; }

                self.value_stream.extend_from_slice(&key_id.to_le_bytes());
                self.value_stream.push(if is_string_type { 1 } else { 0 });
                self.value_stream.extend_from_slice(&raw_data[val_start..val_end]);
                self.value_stream.push(b'\x00');

                while cursor < len && raw_data[cursor] != b',' && raw_data[cursor] != b'}' { cursor += 1; }
                if cursor >= len || raw_data[cursor] == b'}' { break; }
            }
            self.value_stream.extend_from_slice(&0xFFFF_u16.to_le_bytes());
        }

        let out_file = File::create(output_path)?;
        let mut encoder = if len < 50 * 1024 * 1024 {
            zstd::stream::Encoder::new(out_file, 9)?
        } else {
            let mut enc = zstd::stream::Encoder::new(out_file, 11)?;
            enc.multithread(8)?;
            enc
        };
        
        let schema_json = serde_json::to_string(&self.schema_dict)?;
        let schema_bytes = schema_json.as_bytes();

        encoder.write_all(hash_result.as_bytes())?; 
        encoder.write_all(&(schema_bytes.len() as u32).to_le_bytes())?;
        encoder.write_all(schema_bytes)?;
        encoder.write_all(&self.value_stream)?;
        encoder.finish()?;

        Ok(hash_result)
    }

    pub fn decompress(input_path: &str, output_path: &str) -> io::Result<bool> {
        let in_file = File::open(input_path)?;
        let mut decoder = zstd::stream::Decoder::new(in_file)?;
        
        let mut stored_hash_bytes = [0u8; 64];
        decoder.read_exact(&mut stored_hash_bytes)?;
        let stored_hash = std::str::from_utf8(&stored_hash_bytes).unwrap();

        let mut schema_len_bytes = [0u8; 4];
        decoder.read_exact(&mut schema_len_bytes)?;
        let schema_len = u32::from_le_bytes(schema_len_bytes) as usize;
        
        let mut schema_bytes = vec![0u8; schema_len];
        decoder.read_exact(&mut schema_bytes)?;
        let schema_dict: HashMap<String, u16> = serde_json::from_slice(&schema_bytes)?;
        let mut id_to_key: Vec<String> = vec![String::new(); schema_dict.len()];
        for (k, v) in schema_dict { id_to_key[v as usize] = k; }
        
        let mut payload = Vec::new();
        decoder.read_to_end(&mut payload)?;
        
        let out_file = File::create(output_path)?;
        let mut writer = BufWriter::new(out_file);
        let mut cursor = 0;
        let mut first_field = true;

        while cursor < payload.len() {
            let key_id = u16::from_le_bytes([payload[cursor], payload[cursor+1]]);
            cursor += 2;

            if key_id == 0xFFFF {
                writer.write_all(b"}\n")?;
                first_field = true;
                continue;
            }

            if first_field { writer.write_all(b"{")?; first_field = false; }
            else { writer.write_all(b", ")?; }

            let key_name = &id_to_key[key_id as usize];
            writer.write_all(format!("\"{}\": ", key_name).as_bytes())?;

            let val_type = payload[cursor];
            cursor += 1;
            let start = cursor;
            while cursor < payload.len() && payload[cursor] != b'\x00' { cursor += 1; }
            let val = &payload[start..cursor];
            cursor += 1;

            if val_type == 1 {
                writer.write_all(b"\"")?;
                writer.write_all(val)?;
                writer.write_all(b"\"")?;
            } else {
                writer.write_all(val)?;
            }
        }
        writer.flush()?;

        let mut check_file = File::open(output_path)?;
        let mut check_data = Vec::new();
        check_file.read_to_end(&mut check_data)?;
        let mut hasher = Sha256::new();
        hasher.update(&check_data);
        let current_hash = hex::encode(hasher.finalize());

        Ok(current_hash == stored_hash)
    }
}