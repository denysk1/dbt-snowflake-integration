# DBT Snowflake Data Warehouse Project

## Project Structure

**dbt-snowflake-integration/**
**├── analyses/**  # Ad-hoc analyses and one-off queries
**├── macros/** # Reusable SQL/Jinja code blocks
**├── models/** # Core transformation logic
**│ ├── src/ #** Source layer models
**│ ├── stg/** # Staging layer models
**│ ├── int/** # Intermediate layer models
**│ ├── dim/** # Dimension tables
**│ ├── fct/** # Fact tables
**│ └── mart/** # Business-specific data marts
**├── seeds/** # Static data files (CSV)
**├── snapshots/** # Table snapshot definitions
**├── tests/** # Custom data test definitions
**└── dbt_project.yml** # Project configurations


## Data Warehouse Layer Architecture

### RAW Layer
- Original data loaded from source systems
- Never modified directly
- Contains original, unprocessed data
- Example: `PRD_ECOMMERCE_DB.RAW.CUSTOMERS`

### SRC Layer (Source)
- 1:1 mapping with raw tables
- No transformations, just renaming columns for consistency
- Acts as a buffer between raw data and transformations
- Makes it easier to handle source changes
- Example: `src_customers`

### STG Layer (Staging)
- First level of transformations
- Purpose:
  - Data type conversions
  - Column naming standardization
  - Basic data cleaning
  - Handling NULL values
  - Simple calculations
- Example: `stg_customers`

### INT Layer (Intermediate)
- Complex transformations that are reused in multiple models
- Business logic that's too complex for staging
- Prevents duplicate code across dim/fact tables
- Example: `int_customer_orders`

### DIM/FCT Layer
- Dimensional modeling layer
- DIM: Descriptive attributes about business entities
- FCT: Measurable events or transactions
- Examples: 
  - `dim_customers`
  - `fct_orders`

### MART Layer
- Business-specific views combining dims and facts
- Optimized for specific use cases or departments
- Example: `mart_customer_orders`

## Key dbt Commands

### Project Setup & Testing
```bash
# Initialize project
dbt init project_name

# Install dependencies
dbt deps

# Check connection to data warehouse
dbt debug

# Compile SQL without executing
dbt compile
```

### Development Commands
```bash
# Run all models
dbt run

# Run specific models
dbt run --select model_name
dbt run --target dev --select model_name
dbt run --target dev --select path:models/mart

# Run models with tags
dbt run --select tag:daily

# Test all models
dbt test

# Test specific model
dbt test --select model_name

# Generate docs
dbt docs generate
dbt docs serve
```

### Snapshot Commands
```bash
# Run all snapshots
dbt snapshot

# Run specific snapshot
dbt snapshot --select snapshot_name
```

### Common Flags
```bash
# Full refresh (ignore incremental logic)
dbt run --full-refresh

# Run with vars
dbt run --vars '{"var_name": "var_value"}'

# Run with specific target
dbt run --target prod
```

### Environment Setup
```bash
-- Create development environment
CREATE OR REPLACE SCHEMA DEV_ECOMMERCE_DB.RAW CLONE PRD_ECOMMERCE_DB.RAW;
-- Note: This creates a zero-copy clone of the production schema.
-- Find more scripts to set up environment in the sql_scripts folder
```


### Project Configurations
```bash
# profiles.yml
dbt-snowflake-integration:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: your_password
      role: your_role
      database: DEV_ECOMMERCE_DB
      warehouse: your_warehouse
      schema: your_schema
```

### Best Practices
- Always write model documentation
- Use ref() for model references
- Use source() for raw data references
- Include tests for critical models
- Use incremental models for large tables
- Maintain clear naming conventions
- Use tags for model organization

### Testing Strategy
- Schema tests (unique, not null, relationships)
- Custom data tests
- dbt test command
- Snapshot testing
- Data quality checks

### Useful Model Properties
```bash
models:
  - name: model_name
    config:
      materialized: table|view|incremental|ephemeral
      tags: ['daily', 'core']
      schema: target_schema
      unique_key: id_column  # for incremental models
```

### Common Issues and Solutions
- Incremental model issues
    - Use --full-refresh when schema changes
- Snapshot troubleshooting
    - Check unique key constraints
    - Verify timestamp/check columns
- Performance optimization
    - Use appropriate materializations
    - Optimize join orders
    - Consider clustering keys

### Additional Resources
- dbt Documentation
- Snowflake Documentation


### This README provides an  overview of:
- Project structure and organization
- Data warehouse architecture
- Common dbt commands and their usage
- Environment setup
- Best practices
- Testing strategies
- Common issues and solutions
