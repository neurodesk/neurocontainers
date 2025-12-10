# Contributing to NeuroContainers

Thank you for your interest in contributing to NeuroContainers! This document provides guidelines for contributing to the project.

## Getting Started

NeuroContainers is a Python-based automated system for building and testing Docker and Singularity/Apptainer containers for neuroscience applications distributed through NeuroDesk.

### For AI-Powered Development Tools

If you're using GitHub Copilot or other AI-powered coding assistants, comprehensive repository-specific instructions are available in [`.github/copilot-instructions.md`](.github/copilot-instructions.md). These instructions include:

- Complete repository structure and architecture
- Development environment setup and dependencies
- Build system workflows and commands
- Testing and validation procedures
- Code style conventions and best practices
- Common tasks and troubleshooting guides

### Quick Start for Human Contributors

1. **Fork the repository** and clone it locally
2. **Set up the development environment**:
   ```bash
   python3 -m venv env
   source env/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   pip install -e .
   ```
3. **Read the documentation**:
   - [How to Add New Tools](https://www.neurodesk.org/developers/new_tools/) - Tutorial for adding applications
   - [`.github/copilot-instructions.md`](.github/copilot-instructions.md) - Detailed development guide
   - [NeuroContainers Documentation](https://www.neurodesk.org/docs/getting-started/neurocontainers/)

## Contributing Workflow

1. **Create a branch** for your changes from `main`
2. **Make your changes** following the guidelines in `.github/copilot-instructions.md`
3. **Test your changes**:
   ```bash
   # Spell check
   codespell .
   
   # Validate recipes
   source env/bin/activate && ./workflows/test_all.sh
   
   # Build and test specific containers
   python builder/build.py generate <recipe-name> --recreate --build --test
   ```
4. **Commit your changes** with clear, descriptive commit messages
5. **Submit a pull request** with a detailed description of your changes

## Code Style and Quality

- **Python**: Follow PEP 8 style guidelines, use type hints
- **YAML**: Use 2-space indentation, validate syntax before committing
- **Documentation**: Update README files in recipe directories when adding features
- **Commit Messages**: Use descriptive messages that explain what and why

## Adding New Containers

To add a new neuroscience tool to NeuroContainers:

1. Initialize a new recipe:
   ```bash
   sf-init <toolname> <version>
   ```
2. Edit `recipes/<toolname>/build.yaml` to define the container
3. Validate and test:
   ```bash
   python builder/build.py generate <toolname> --recreate --check-only
   python builder/build.py generate <toolname> --recreate --build --test
   ```

For detailed instructions, see the [Recipe Development Guidelines](https://www.neurodesk.org/developers/new_tools/) and [`.github/copilot-instructions.md`](.github/copilot-instructions.md).

## Pull Request Guidelines

- Keep PRs focused on a single issue or feature
- Ensure all CI checks pass
- Include tests for new functionality
- Update documentation as needed
- Respond to review feedback promptly

## Security

- Pin versions for all dependencies
- Use HTTPS for all downloads
- Review upstream changes before updating versions
- Document licensing information (SPDX identifiers)
- Never include secrets or credentials in containers

## Getting Help

- Check [`.github/copilot-instructions.md`](.github/copilot-instructions.md) for detailed guidance
- Review existing recipes in `recipes/` for examples
- Consult the [NeuroDesk documentation](https://www.neurodesk.org/docs/)
- Check GitHub Actions logs for CI failure details

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please be respectful and considerate in all interactions.

## License

By contributing to NeuroContainers, you agree that your contributions will be licensed under the same license as the project.
