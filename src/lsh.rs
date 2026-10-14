pub struct LshRouter {
    dim: usize,
    hyperplanes: Vec<Vec<f32>>,
}

impl LshRouter {
    pub fn new(dim: usize) -> Self {
        let mut hyperplanes = Vec::with_capacity(16);
        let mut seed: u64 = 0x5DEECE66D;
        for _ in 0..16 {
            let mut plane = Vec::with_capacity(dim);
            for _ in 0..dim {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let val = ((seed >> 33) as i32 as f32) / (i32::MAX as f32);
                plane.push(val);
            }
            hyperplanes.push(plane);
        }
        Self { dim, hyperplanes }
    }

    pub fn embed_text(&self, text: &str) -> Vec<f32> {
        let mut vec = vec![0.0f32; self.dim];
        let cleaned = text.to_lowercase();
        let tokens: Vec<&str> = cleaned.split(|c: char| !c.is_alphanumeric()).filter(|s| !s.is_empty()).collect();
        for (i, token) in tokens.iter().enumerate() {
            let h1 = Self::fnv1a(token.as_bytes());
            let idx1 = (h1 as usize) % self.dim;
            vec[idx1] += if (h1 >> 32) & 1 == 0 { 1.0 } else { -1.0 };
            let bytes = token.as_bytes();
            if bytes.len() >= 3 {
                for w in bytes.windows(3) {
                    vec[(Self::fnv1a(w) as usize) % self.dim] += 0.5;
                }
            }
            if i + 1 < tokens.len() {
                let bg = format!("{}_{}", token, tokens[i + 1]);
                vec[(Self::fnv1a(bg.as_bytes()) as usize) % self.dim] += 0.75;
            }
        }
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 { for x in vec.iter_mut() { *x /= norm; } }
        vec
    }

    pub fn hash_vector(&self, vec: &[f32]) -> u16 {
        let mut sig: u16 = 0;
        for (i, plane) in self.hyperplanes.iter().enumerate() {
            let dot: f32 = vec.iter().zip(plane.iter()).map(|(a, b)| a * b).sum();
            if dot >= 0.0 { sig |= 1 << i; }
        }
        sig
    }

    fn fnv1a(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in bytes { hash ^= b as u64; hash = hash.wrapping_mul(0x100000001b3); }
        hash
    }
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { 0.0 } else { dot / (na * nb) }
}
