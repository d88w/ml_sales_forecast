📈 Company Sales Forecast – Machine Learning

A supervised learning project that uses historical Salesforce pipeline data to forecast quarterly bookings.

🔍 Overview

This project builds machine learning models to predict end-of-quarter sales bookings using data from Salesforce (opportunity_history). It demonstrates a full ML pipeline from data extraction to model deployment.

⚙️ Key Components
- Data Source: Historical Salesforce pipeline data
- ETL & Feature Engineering:
  - SQL for extracting and transforming data
  - NumPy, Pandas, and scaling techniques for feature engineering
- Modeling:
  - Multiple supervised learning models built using scikit-learn
  - Each model lives in its own notebook for modular experimentation
- Model Persistence:
  - Models are saved and loaded using pickle

🧪 Use Cases
- Forecasting sales performance before quarter-end
- Identifying leading indicators of pipeline health
- Supporting RevOps and sales leadership decision-making
