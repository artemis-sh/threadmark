use ulid::Ulid;

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Ulid::new().to_string().to_lowercase())
}
