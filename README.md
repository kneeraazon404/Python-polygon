# Python-Polygon 🚀

A Python-based framework for building historical stock market screeners with seamless integration to [Polygon.io](https://polygon.io/). This project focuses on querying, processing, and filtering financial data efficiently, with a particular emphasis on market capitalization and other screening parameters.

---

## 🌟 Features

- **Historical Data Retrieval**:
  Query historical data from [Polygon.io](https://polygon.io) with robust and efficient methods.
  
- **Market Capitalization Integration**:
  Fetch and filter data by market cap for better insights and screening capabilities.

- **Customizable Screeners**:
  Define and execute your own screening logic with modular components.

- **Optimized for AWS Deployment**:
  Designed to handle large datasets efficiently, with performance optimizations for cloud environments.

---

## 🛠️ Project Structure

The project is composed of multiple Python modules, each serving a specific purpose:

### 1. **`main_sc.py`**
   - The entry point of the application.
   - Sets up date parameters, tasks, and initializes the entire screener workflow.

### 2. **`parabscanner.py`**
   - Executes scanner operations, including:
     - Data filtering.
     - Sorting by market capitalization or other metrics.
   - Includes logic for integrating market capitalization data.

### 3. **`scanner.py`**
   - Defines an abstract scanner class.
   - Provides the basic structure and methods for implementing custom scanner logic.

### 4. **`ticker_sc.py`**
   - Handles data queries to [Polygon.io](https://polygon.io), including:
     - Data cleanup.
     - Pulling ticker details like market capitalization.
   - **Key Area for Enhancement**:
     - Improve querying efficiency to minimize API calls.
     - Ensure seamless integration of market cap data for downstream operations.

---

## 🔑 Key Requirements

This project focuses on improving and enhancing the following areas:

1. **Efficient Market Cap Integration**:
   - Refine the `ticker_sc.py` logic to efficiently pull ticker details (e.g., market capitalization) from Polygon.io.
   - Ensure minimal and optimized API querying.

2. **Performance Improvements**:
   - Address inefficiencies in `parabscanner.py` to filter and sort data by market capitalization more effectively.
   - Optimize for deployment in AWS environments with large datasets.

3. **Follow Existing Code Patterns**:
   - Maintain consistency with the existing code structure to ensure readability and maintainability.

---

## 🚀 How to Get Started

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/kneeraazon404/Python-polygon.git
   cd Python-polygon
   ```

2. **Install Dependencies**:
   - Create and activate a Python virtual environment:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
   - Install required packages:
     ```bash
     pip install -r requirements.txt
     ```

3. **Set Up Polygon.io API Key**:
   - Obtain an API key from [Polygon.io](https://polygon.io).
   - Add your API key to the environment or a `.env` file:
     ```env
     POLYGON_API_KEY=your_api_key_here
     ```

4. **Run the Application**:
   ```bash
   python main_sc.py
   ```

---

## 📈 Example Usage

- **Filter Tickers by Market Capitalization**:
  Customize `parabscanner.py` to filter stocks by market cap thresholds.

- **Integrate Historical Data**:
  Use `ticker_sc.py` to query and analyze historical stock data.

- **Build Your Own Screener**:
  Extend `scanner.py` to define your custom screening logic.

---

## 🛡️ Testing and Debugging

- Run unit tests to validate the functionality of individual components:
  ```bash
  pytest tests/
  ```

- Debug API queries with verbose logging:
  ```bash
  python main_sc.py --debug
  ```

---

## 🌐 Deployment

This project is optimized for deployment in cloud environments such as AWS. Key considerations for deployment:

- Use AWS Lambda or EC2 for running screeners.
- Leverage S3 for storing large datasets.
- Optimize API calls to reduce latency and costs.

---

## 🤝 Contributions

Contributions are welcome! Follow these steps to contribute:

1. Fork the repository.
2. Create a new branch:
   ```bash
   git checkout -b feature-name
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add feature-name"
   ```
4. Push to your fork and create a pull request.

---

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

