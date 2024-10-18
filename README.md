# Solana Indexer

![Solana data relational model](solana_data_relational_model.png)

## ! Currently porting from Python to Rust !
- Branch [`rust_port`](https://github.com/lgingerich/solana-indexer/tree/rust_port)

## Development

### Setting Up the Development Environment

To install dependencies, run:

```bash
poetry install
```

This will install all the dependencies defined in your `pyproject.toml` file.

### Activating the Virtual Environment

To activate the project's virtual environment, run:

```bash
poetry shell
```

This will spawn a shell with the virtual environment activated, allowing you to run 
Python scripts and commands within the virtual environment context.

### Running the Indexer with Poetry

To run the indexer using Poetry, use:

```bash
poetry run python main.py
```

### Running with Docker

1. Build the Docker image:
   ```bash
   docker build -t solana-indexer .
   ```

2. Run the container:
   ```bash
   docker run solana-indexer
   ```

### Deploying with Terraform on AWS

1. Install Terraform and configure your AWS CLI with appropriate credentials.

2. Generate an SSH key pair for EC2 instance access:
   ```bash
   ssh-keygen -t rsa -b 4096 -f ~/.ssh/solana-indexer-key
   ```

3. Initialize Terraform:
   ```bash
   terraform init
   ```

4. Apply the Terraform configuration:
   ```bash
   terraform apply
   ```

Note: Currently in the process of moving from using AWS access keys in `terraform.tfvars` to AWS Secrets Manager for improved security. The current `terraform.tfvars.example` file will be deprecated soon.

### Running Tests

To run the test suite with pytest, use:

```bash
poetry run pytest
```

This will execute all tests found in the `tests/` directory.

### Formatting Code

To format your code automatically with Black, run:

```bash
poetry run black .
```

Black will reformat your files in place to adhere to its style guide.

### Linting Code

To lint your code with Ruff, run:

```bash
poetry run ruff .
```

Ruff will analyze your code for potential errors and style issues.

## To Do:

- Some slots/blocks are missing block time data. Want to partition on this field, so 
need to fill in the missing data.

- Fix partitioning in iceberg table creation

- Fix schema definition for nullable fields

- Parallel processing for historical backfills

- Handling restarts
    - Need to decide how to handle data restarts
        - If a single table has data, what slot should it start from?

## License

This project is licensed under the [MIT License](LICENSE).