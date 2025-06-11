# Data Warehouse Project

A complete data warehousing solution using PostgreSQL, SQLAlchemy, and Python.

## Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Git

### Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL
docker-compose up -d

# Initialize database
python scripts/setup_database.py

# Run ETL pipeline
python main.py

# Launch dashboard
streamlit run dashboard.py
```

## Project Architecture

### Core Components
- **Data Warehouse**: PostgreSQL database with star schema design
- **ETL Pipeline**: Python-based extraction, transformation, and loading
- **Analytics Dashboard**: Streamlit web application
- **Data Models**: SQLAlchemy ORM for database interactions

### Technology Stack
- **Database**: PostgreSQL
- **Backend**: Python, SQLAlchemy
- **Frontend**: Streamlit
- **Containerization**: Docker, Docker Compose
- **Data Processing**: Pandas, NumPy

### Project Structure
```
data_warehouse_project/
├── src/
│   ├── models/          # Database models
│   ├── etl/            # ETL pipeline scripts
│   └── utils/          # Utility functions
├── scripts/            # Setup and maintenance scripts
├── dashboard.py        # Streamlit dashboard
├── main.py            # Main application entry point
├── requirements.txt    # Python dependencies
└── docker-compose.yml  # Docker configuration
```

## Features

### Data Warehouse Capabilities
- Star schema dimensional modeling
- Fact and dimension tables
- Data quality validation
- Historical data tracking
- Automated data lineage

### ETL Pipeline
- Scheduled data extraction
- Data transformation and cleansing
- Incremental loading strategies
- Error handling and logging
- Data validation checks

### Analytics Dashboard
- Interactive data visualizations
- Real-time metrics monitoring
- Custom report generation
- Export functionality
- User-friendly interface

## Installation Guide

### Step 1: Environment Setup
```bash
git clone https://github.com/yourusername/data-warehouse-project.git
cd data-warehouse-project
python -m venv venv
source venv/bin/activate
```

### Step 2: Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Database Setup
```bash
docker-compose up -d postgresql
python scripts/setup_database.py
```

### Step 4: Data Loading
```bash
python main.py --load-sample-data
```

### Step 5: Dashboard Launch
```bash
streamlit run dashboard.py
```

## Configuration

### Database Configuration
Edit `config/database.yaml`:
```yaml
database:
  host: localhost
  port: 5432
  name: data_warehouse
  username: postgres
  password: your_password
```

### ETL Configuration
Edit `config/etl.yaml`:
```yaml
etl:
  batch_size: 1000
  parallel_jobs: 4
  retry_attempts: 3
```

## Usage Examples

### Running ETL Pipeline
```bash
# Full refresh
python main.py --mode full

# Incremental update
python main.py --mode incremental

# Specific date range
python main.py --start-date 2024-01-01 --end-date 2024-01-31
```

### Database Operations
```python
from src.models import FactSales, DimCustomer
from src.utils.database import get_session

# Query sales data
session = get_session()
sales = session.query(FactSales).all()

# Customer analysis
customers = session.query(DimCustomer).filter(
    DimCustomer.region == 'North America'
).all()
```

## Monitoring and Maintenance

### Health Checks
- Database connectivity monitoring
- ETL pipeline status tracking
- Data quality metrics
- Performance monitoring

### Backup Strategy
- Automated daily backups
- Point-in-time recovery
- Cross-region replication
- Disaster recovery procedures

## Contributing

### Development Setup
1. Fork the repository
2. Create feature branch
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Run tests: `pytest`
5. Submit pull request

### Code Standards
- Follow PEP 8 style guide
- Write comprehensive tests
- Document all functions
- Use type hints

## Troubleshooting

### Common Issues
- **Database connection errors**: Check Docker container status
- **Memory issues**: Adjust batch size in ETL configuration
- **Performance problems**: Review query optimization
- **Data quality issues**: Check validation rules

### Support
- Check documentation in `docs/` directory
- Review log files in `logs/` directory
- Submit issues on GitHub
- Contact support team

## License
MIT License - see LICENSE file for details