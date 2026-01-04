# Repository Guidelines

## Project Structure & Module Organization
- Multi-module Go repo: root module plus submodules in `cmd/kratos`, `cmd/protoc-gen-go-*`, and many `contrib/**/go.mod` directories.
- Core packages live at the repo root: `transport/`, `middleware/`, `config/`, `registry/`, `encoding/`, `log/`, `errors/`, and `metadata/`.
- Protobuf definitions and generated code are in `api/` and `third_party/`.
- Internal-only code and fixtures live under `internal/` (including `internal/testdata/`).
- Documentation and design notes are in `docs/` and top-level `README.md`.

## Build, Test, and Development Commands
- `make all`: build the CLI binaries under `cmd/kratos`, `cmd/protoc-gen-go-errors`, and `cmd/protoc-gen-go-http`.
- `make install`: install built binaries to `GOBIN` or `GOPATH/bin` and ensure required `protoc` plugins are present.
- `make lint`: run `golangci-lint` across all modules using `.golangci.yml`.
- `make fix`: apply automated lint fixes where possible.
- `make test`: run `go test -race ./...` per module.
- `make test-coverage`: write combined coverage to `coverage.out`.
- `make proto`: regenerate protobuf stubs for core protos.
- `make clean`: run `go mod tidy` across modules.

## Coding Style & Naming Conventions
- Format with `gofmt`, `gofumpt`, and `goimports` (configured via `golangci-lint`); import path prefix is `github.com/go-kratos`.
- Keep line length at or under 160 characters (per `lll` linter).
- Follow Go naming: `CamelCase` for exported identifiers, `mixedCaps` for unexported, lowercase package names.
- Tests live in `*_test.go` with `TestXxx` and `BenchmarkXxx` naming.

## Testing Guidelines
- Use the standard `testing` package; favor table-driven tests for edge coverage.
- Run `make test` locally before PRs; use `make test-coverage` for coverage output.
- Ensure new features include tests (required by the contribution guide).

## Commit & Pull Request Guidelines
- Commit messages follow Conventional Commits, e.g. `fix(log): handle nil logger`.
- Types include `feat`, `fix`, `docs`, `refactor`, `style`, `test`, `chore`, `ci`, `deps`, `break`; add optional scope like `transport` or `middleware`.
- Use imperative, lowercase descriptions with no trailing period; include `!` or `BREAKING CHANGE` when applicable.
- PRs should link relevant issues (proposal/feature for new features), describe changes, and mention test coverage.

## Security & Configuration Tips
- Report security issues via `SECURITY.md` and follow the process there.
- When working in submodules, run commands from the module root (e.g., `contrib/registry/etcd`).
