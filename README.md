# Ingestion Framework

A Python-based ETL framework for extracting, transforming, validating, and loading data, with support for batch processing, web scraping, data quality checks, and reconciliation workflows.

## Features

- **ETL Pipeline:** Organizes data extraction, transformation, and loading.
- **Batch Processing:** Supports batch jobs, scheduling, and worker management.
- **Web Scraping:** Provides browser-based and web data extraction capabilities.
- **Data Quality:** Supports configurable data quality rules and validation.
- **SCD Type 2:** Supports historical data tracking through SCD Type 2 loading.
- **Loading Strategies:** Includes append, truncate, and other configurable loading approaches.
- **Reconciliation:** Supports data reconciliation workflows.
- **Logging and Monitoring:** Provides logging utilities and health checks.
- **Configuration Management:** Separates application configuration from sensitive credentials.

## Project Structure

```text
ingestion-framework/
├── Batch/                 # Batch processing and worker management
├── Extract/               # Data extraction
├── Transform/             # Data transformation
├── Load/                  # Data loading
├── load_handlers/         # Loading strategies
├── dq/                    # Data quality processing
├── recon/                 # Data reconciliation
├── conf/                  # Configuration utilities
├── decorators/            # Reusable decorators
├── dltsample/             # DLT-related examples or components
├── misc/                  # Supporting documentation
├── utils/                 # Database, logging, and utility modules
├── main.py                # Main ETL entry point
├── main_batch_run.py      # Batch execution entry point
├── main_web_scraper.py    # Web scraping entry point
├── etl_runner.py          # ETL execution utilities
├── scraper.py             # Scraping implementation
├── requirements.txt       # Python dependencies
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.x compatible with the project's dependencies
- pip
- Git
- Access to the required source and target databases
- Browser dependencies, if required by the scraping components

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/prakashpandey16/Ingestion-framework.git
   cd Ingestion-framework
   ```

2. Create a virtual environment:

   ```bash
   python -m venv .venv
   ```

3. Activate the environment.

   **Windows (Command Prompt):**

   ```cmd
   .venv\Scripts\activate
   ```

   **Linux/macOS:**

   ```bash
   source .venv/bin/activate
   ```

4. Install dependencies:

   ```bash
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

## Configuration

Before running the framework:

1. Configure the required database connections and application settings.
2. Provide credentials through environment variables or a secure local configuration mechanism supported by the application.
3. Configure the required data quality rules.
4. Ensure that database access, file paths, and external service credentials are available in the execution environment.

**Security:** Do not commit passwords, API keys, AWS credentials, private keys, browser session files, or other sensitive information. Keep local configuration and secret files out of version control.

## Usage

The following commands correspond to the available entry-point scripts. Check each script's arguments and configuration requirements before running it.

### Run the ETL Pipeline

```bash
python main.py
```

### Run Batch Processing

```bash
python main_batch_run.py
```

Supply a job ID only if the script expects one.

### Run Web Scraping

```bash
python main_web_scraper.py
```

## Core Components

### Extract, Transform, and Load

Coordinates data extraction, transformation, and loading through dedicated modules.

### Batch Processing

Provides components for managing batch execution, workers, scheduling, and logs.

### Data Quality

Contains data quality processing and configuration components.

### Reconciliation

Supports reconciliation between datasets or processing stages.

### Loading Strategies

Includes modules for append, truncate, and SCD Type 2 loading, along with shared loading utilities.

### Utilities

Provides supporting functionality for database configuration, logging, encryption, health checks, and data processing.

## Development

1. Create a feature branch.
2. Implement and test your changes.
3. Run relevant tests and validation checks.
4. Commit your changes with a descriptive message.
5. Push your branch and open a pull request.

## License

Add the applicable license information here. If you intend to use the MIT License, include a valid `LICENSE` file containing the MIT license text.
