# NHL Data Pipeline & Analytics
Hey folks, my name is Cullen. I've spent the last 10 years as a software engineer focused on the Microsoft toolchain for my job. In an effort to learn some industry standard tools, I'm building a project that mixes my passions. The NHL and data!

In the effort to learn these tools in a serious context, some choices may seem inefficient or overkill for a personal project. But, I am (mostly) trying to model how a large corporation would use these tools.

Expect updates to this README as I learn some lessons and play with the scope.

## Project Overview
This project is a comprehensive data engineering pipeline designed to ingest, process, and analyze NHL game data. 

**Primary Goal:**  
The short-term objective is to generate predictive reports for NHL players, specifically focusing on **Shots on Goal (SOG)** projections. This will serve as a foundation for analyzing player props and performance trends.

**Future Roadmap:**
- Expand analysis to other statistical categories (Goals, Assists, Time on Ice, etc.).
- Implement a full CI/CD lifecycle with distinct Development and Production environments.
- Integrate advanced visualization dashboards.
- Implement ML/AI tooling for further analysis.

## Technology Stack
This project serves as a practical playground for mastering modern data engineering technologies:

*   **Languages:** Python, SQL
*   **Development Environment:** VS Code with WSL (Ubuntu)
*   **Orchestration:** Apache Airflow
*   **Cloud Infrastructure:** AWS (S3, IAM)
*   **Data Warehousing:** Snowflake
*   **Transformation:** dbt (Data Build Tool)
*   **Processing:** PySpark (planned)
*   **Visualization:** Looker / Tableau (planned)

## Architecture & Infrastructure

### Cloud Setup (AWS)
The project leverages AWS for scalable storage and security:
*   **MWAA (Managed Workflows for Apache Airflow):** Used for hosting the Airflow environment in the cloud.
*   **S3:** Acts as the Data Lake, storing raw JSON responses from the NHL API (Schedule, Boxscores, Play-by-Play, Skater Reports).
*   **IAM:** Granular Identity and Access Management roles are being implemented to secure services, though some components currently use broader permissions during the development phase.

### Environment Strategy
*   **Current State:** The project is currently operating in a **Development** environment.
*   **Future State:** A formal promotion strategy (Dev &rarr; Stage &rarr; Prod) is planned to ensure reliability and data quality.

## CI/CD & Quality Assurance
To maintain code quality and ensure reliable deployments, the project utilizes a structured CI/CD pipeline:

*   **GitHub Actions:** Automated workflows trigger on pull requests and pushes to `main` and `develop` branches.
    *   **Linting:** Code is checked with `ruff` to enforce style guidelines.
    *   **Testing:** Unit tests are executed via `pytest` to verify ingestion logic.
*   **Pull Request Process:**
    *   All changes require a Pull Request (PR) to be merged.
    *   **GitHub Copilot:** Leveraged to assist with PR descriptions, code reviews, and generating test cases.

## Current Progress

### 1. Data Ingestion Layer
Built a robust Python-based ingestion module (`src/nhl_pipeline/ingestion`) that interacts with public NHL APIs.
*   **Schedule Fetcher:** Retrieves daily game schedules.
*   **Game Data:** Fetches detailed Boxscores and Play-by-Play (PBP) data.
*   **Skater Reports:** Captures granular stats for individual skaters.
*   **S3 Integration:** All raw data is automatically uploaded to an S3 Data Lake.

### 2. Orchestration (Airflow)
Airflow DAGs have been deployed to automate the workflow:
*   `nhl_daily_ingestion_dag`: Runs daily to fetch the latest game data.
*   `nhl_backfill_dag`: Handles historical data loading to populate the warehouse.
*   `nhl_raw_stats_skater_daily`: Specialized pipeline for daily skater statistics.

### 3. Transformation (dbt)
Initial dbt models are set up to transform raw data into analytical tables:
*   **Staging:** `stg_games`, `stg_player_game_stats` clean and normalize raw data.
*   **Analytics:** `fact_player_game_stats` aggregates player performance for downstream analysis.

## Getting Started

### Prerequisites
*   Python 3.x
*   AWS CLI configured

### Setup
Use the provided Makefile to set up the environment. This runs `setup_wsl.sh` to configure system dependencies and `setup_venv.sh` to initialize the Python virtual environment:
```bash
make setup
```

### Testing
Run the test suite to ensure ingestion logic is working correctly:
```bash
make test
```

