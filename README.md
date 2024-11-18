# Tubes-SBD-MarioDB (Query Optimizer)

## Setup Instructions

Follow the steps below to set up the project and run tests.

---

### 1. Create a Virtual Environment

#### On Linux/Mac:
```python3 -m venv venv```

#### On Windows:
```python -m venv venv```

This will create a virtual environment named `venv` in your project directory.

---

### 2. Activate the Virtual Environment

#### On Linux/Mac:
```source venv/bin/activate```

#### On Windows (Command Prompt):
```venv\Scripts\activate```

#### On Windows (PowerShell):
```.\venv\Scripts\activate```

Once activated, your terminal prompt should show the environment name (e.g., `(venv)`).

---

### 3. Install Required Packages
After activating the virtual environment, install the dependencies from `requirements.txt`:
```pip install -r requirements.txt```

---

### 4. Run Tests
To run the tests, use the following command:
```pytest```

This will execute all the tests in the project.

---

### Notes
- Ensure that `python` and `pip` point to the correct version of Python. Use `python --version` or `python3 --version` to verify.
- If `pytest` is not recognized, ensure it is installed by running:
  ```pip install pytest```

---

### Additional Commands
- To deactivate the virtual environment:
  ```deactivate```
- To update dependencies:
  ```pip freeze > requirements.txt```

---
