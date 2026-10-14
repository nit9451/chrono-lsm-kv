use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub enum WalOp {
    Fork {
        parent: u64,
        new_branch: u64,
        ts: u64,
    },
    Commit {
        tx_id: u64,
        branch_id: u64,
        state_tag: u8,
        confidence_bits: u32,
        deps: Vec<u64>,
        payload: String,
    },
}

pub struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn append(&mut self, op: &WalOp) -> io::Result<()> {
        let body = Self::encode_op(op);
        let crc = Self::crc32(&body);
        let len = body.len() as u32;

        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn recover<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalOp>> {
        if !path.as_ref().exists() {
            return Ok(Vec::new());
        }
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut ops = Vec::new();
        let mut cursor = 0;

        while cursor + 8 <= buf.len() {
            let expected_crc = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap());
            let len = u32::from_le_bytes(buf[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
            cursor += 8;

            if cursor + len > buf.len() {
                break;
            }

            let body = &buf[cursor..cursor + len];
            let actual_crc = Self::crc32(body);
            if expected_crc != actual_crc {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL CRC32 mismatch"));
            }

            if let Some(op) = Self::decode_op(body) {
                ops.push(op);
            }
            cursor += len;
        }
        Ok(ops)
    }

    fn encode_op(op: &WalOp) -> Vec<u8> {
        let mut out = Vec::new();
        match op {
            WalOp::Fork { parent, new_branch, ts } => {
                out.push(0x01);
                out.extend_from_slice(&parent.to_le_bytes());
                out.extend_from_slice(&new_branch.to_le_bytes());
                out.extend_from_slice(&ts.to_le_bytes());
            }
            WalOp::Commit {
                tx_id,
                branch_id,
                state_tag,
                confidence_bits,
                deps,
                payload,
            } => {
                out.push(0x02);
                out.extend_from_slice(&tx_id.to_le_bytes());
                out.extend_from_slice(&branch_id.to_le_bytes());
                out.push(*state_tag);
                out.extend_from_slice(&confidence_bits.to_le_bytes());
                out.extend_from_slice(&(deps.len() as u32).to_le_bytes());
                for &d in deps {
                    out.extend_from_slice(&d.to_le_bytes());
                }
                let bytes = payload.as_bytes();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
        out
    }

    fn decode_op(body: &[u8]) -> Option<WalOp> {
        if body.is_empty() {
            return None;
        }
        match body[0] {
            0x01 if body.len() >= 25 => {
                let parent = u64::from_le_bytes(body[1..9].try_into().ok()?);
                let new_branch = u64::from_le_bytes(body[9..17].try_into().ok()?);
                let ts = u64::from_le_bytes(body[17..25].try_into().ok()?);
                Some(WalOp::Fork { parent, new_branch, ts })
            }
            0x02 if body.len() >= 26 => {
                let tx_id = u64::from_le_bytes(body[1..9].try_into().ok()?);
                let branch_id = u64::from_le_bytes(body[9..17].try_into().ok()?);
                let state_tag = body[17];
                let confidence_bits = u32::from_le_bytes(body[18..22].try_into().ok()?);
                let dep_count = u32::from_le_bytes(body[22..26].try_into().ok()?) as usize;
                let mut pos = 26;
                let mut deps = Vec::with_capacity(dep_count);
                for _ in 0..dep_count {
                    let d = u64::from_le_bytes(body[pos..pos + 8].try_into().ok()?);
                    deps.push(d);
                    pos += 8;
                }
                let pay_len = u32::from_le_bytes(body[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let payload = String::from_utf8_lossy(&body[pos..pos + pay_len]).to_string();
                Some(WalOp::Commit {
                    tx_id,
                    branch_id,
                    state_tag,
                    confidence_bits,
                    deps,
                    payload,
                })
            }
            _ => None,
        }
    }

    fn crc32(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFFFFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                if (crc & 1) != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }
}
