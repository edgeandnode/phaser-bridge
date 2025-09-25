use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvmCommonError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Invalid block data: {0}")]
    InvalidBlock(String),

    #[error("Conversion error: {0}")]
    Conversion(String),
}

pub type Result<T> = std::result::Result<T, EvmCommonError>;
