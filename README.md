# Apple-Data-Analysis-Using-Apache-Spark_PY
### README.md

# ETL Workflows for Customer Transactions

This repository contains two ETL (Extract, Transform, Load) workflows implemented using PySpark. The workflows are designed to process customer transaction data and generate insights on customers who have bought AirPods after purchasing an iPhone and customers who have bought only iPhones.

## Workflows Overview

### FirstWorkFlow
This workflow processes data to identify customers who have bought AirPods just after buying an iPhone.

1. **Extraction**:
   - Extracts transaction data from a CSV file.
   - Extracts customer data from a Delta table.

2. **Transformation**:
   - Transforms the extracted data to identify customers who bought AirPods after purchasing an iPhone.

3. **Loading**:
   - Loads the transformed data to the specified sink.

### SecondWorkFlow
This workflow processes data to identify customers who have bought only iPhones.

1. **Extraction**:
   - Reuses the extracted data from `FirstWorkFlow`.

2. **Transformation**:
   - Transforms the data to identify customers who have bought only iPhones.

3. **Loading**:
   - Loads the transformed data to the specified sink.

## Classes and Methods

### Extractor
An abstract class to define the structure for data extraction.

### AirpodsAfterIphoneExtractor
Inherits from `Extractor` and implements the `extract` method to extract transaction and customer data.

### AirpodsAfterIphoneTransformer
Contains the transformation logic to identify customers who bought AirPods after purchasing an iPhone.

### OnlyAipodsAndIphone
Contains the transformation logic to identify customers who bought only iPhones.

### AirPodsAfterIphoneLoader
Handles loading the transformed data to the specified sink.

### OnlyAirPodsAndIphoneLoader
Handles loading the transformed data to the specified sink.

### FirstWorkFlow
Implements the ETL pipeline to process data for customers who bought AirPods after purchasing an iPhone.

### SecondWorkFlow
Implements the ETL pipeline to process data for customers who bought only iPhones.

### WorkFlowRunner
A runner class to execute the specified workflow.

## Usage

To run the workflows, you can instantiate the `WorkFlowRunner` class with the desired workflow name and call the `runner` method.

```python
name = "secondWorkFlow"
workflow_runner = WorkFlowRunner(name).runner()
```

## Example

### FirstWorkFlow

```python
first_workflow = FirstWorkFlow()
first_workflow.runner()
```

### SecondWorkFlow

```python
input_dfs = FirstWorkFlow().runner()
second_workflow = SecondWorkFlow()
second_workflow.runner(input_dfs)
```

## Prerequisites

- Apache Spark
- PySpark
- Databricks environment (or any other Spark-compatible environment)

## Installation

Clone the repository and set up your environment to run the workflows.

```bash
git clone https://github.com/yourusername/etl-workflows.git
cd etl-workflows
```

Ensure your environment is configured with Spark and PySpark.

## Contributing

Contributions are welcome! Please create a pull request with detailed information on the changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For any inquiries, please reach out to [abhishekmandge@gmail.com],

[LinkedIn](https://www.linkedin.com/in/abhishek-mandge/)

---
