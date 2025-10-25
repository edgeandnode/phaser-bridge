# Phaser Query Development Guide

## Development Guidelines

### Solution Discussion
When working on new features or changes:
- First explain what currently exists in the codebase
- Describe specifically what would need to be implemented
- Clarify the technical approach before proceeding
- Don't over-analyze alternatives - focus on understanding the specific requirement

## Testing Guidelines

- While in the process of building and debugging, don't use the --release flag unless asked.

### Where do I put tests?
**DO NOT create new binary files to test functionality unless directly asked.** Instead:
- Write unit tests or integration tests
- Test functionality directly in the main binary/application
- Use existing test infrastructure

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

## Git Commit Guidelines

### Commit Messages
- Write clear, concise commit messages that explain the "why" not just the "what"
- Use conventional commit format when appropriate
- Due to GPG signing requirements, provide commit scripts for the user to execute themselves

### Atomic Commits
**IMPORTANT**: Each commit should contain ONE logical change. Multiple related files can be in one commit if they're part of the same logical change.

When there are multiple unrelated changes in the working directory:
- Analyze the git diff and identify logical groupings
- Create a shell script named `commit-changes.sh` (gitignored) with separate commits for each logical change
- Each commit in the script should be atomic (one logical change)
- Common groupings:
  - Feature additions (separate commit per feature)
  - Bug fixes (separate commit per fix)
  - Logging/debugging improvements
  - Code formatting (cargo fmt)
  - Documentation updates
- Always make formatting commits last to keep the history clean

### Commit Script Format
When creating commit scripts:
- Make the script executable: `chmod +x commit-changes.sh`
- Include `set -e` at the top to fail on errors
- Add echo statements to announce what's being committed
- Show how to review and push at the end
- **NEVER** include Claude attribution, Co-Authored-By, or "Generated with Claude Code"
- Keep commit messages clean and professional

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

## Error Handling Guidelines

### Result Type Aliases
**NEVER** create `Result` type aliases over `std::result::Result`. This causes:
- Type confusion between different Result types
- Incompatible trait implementations
- Breaking changes when error boundaries need different error types

Instead:
- Use bare `Result<T, ErrorType>` (from prelude) in function signatures
- **NEVER** import any Result types without `as` - always use `as` (e.g., `use anyhow::Result as AnyhowResult`)
- Use appropriate error types (like `tonic::Status`) at API boundaries
- Implement `From` traits for automatic error conversion with `?` operator
- Import conflicting types with `as` (e.g., `use tonic::Status as TonicStatus`)
