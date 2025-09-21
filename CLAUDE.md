# Phaser Query Development Guide

## Testing Guidelines

### Test Data Directory
All test data should be written to the `test-data/` directory, which is already in `.gitignore`. This ensures test artifacts don't get committed to the repository.

Example usage:
```bash
./target/debug/test-dual-write http://127.0.0.1:8090 ./test-data
```

## Code Quality Checks

### Linting and Type Checking
Before completing any task, run the following commands to ensure code quality:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
cargo check --all-targets
```

## Architecture Notes

### Bridges
- Bridges should be stateless protocol translators
- No caching or buffering in bridges - that's the responsibility of phaser-query
- Bridges convert between node protocols (e.g., Erigon gRPC) and Arrow Flight

### Data Storage
- Use RocksDB column families for temporary buffering
- Write to `.tmp` files during active writes
- Rename to `.parquet` only after successful flush and rotation
- Dual-write strategy: Write to both RocksDB CF and Parquet simultaneously