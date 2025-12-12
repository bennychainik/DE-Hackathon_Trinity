# Git Collaboration Strategy

## Branching Strategy
We use a simplified Feature Branch workflow:
- **`main`**: Production-ready code. ALWAYS deployable.
- **`dev`**: Integration branch. PRs from feature branches merge here first.
- **`feat/feature-name`**: New features (e.g., `feat/ingestion-pipeline`).
- **`fix/bug-name`**: Bug fixes.

## Commit Messages
Follow [Conventional Commits](https://www.conventionalcommits.org/):
- `feat: add CSV ingestion support`
- `fix: resolve null pointer in validation`
- `docs: update data dictionary`
- `chore: update requirements.txt`

## Workflow
1. **Pull Latest**: `git checkout main && git pull`
2. **Create Branch**: `git checkout -b feat/my-task`
3. **Work & Commit**: `git add .` -> `git commit -m "feat: output"`
4. **Push**: `git push -u origin feat/my-task`
5. **PR**: Open Pull Request to `main` (or `dev`).
