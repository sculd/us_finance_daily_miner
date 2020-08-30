## First we import the required packages:
import numpy as np, pandas as pd
from datetime import timedelta
import time
from feature_selector import FeatureSelector
import datetime
import pickle
import progressbar
from sklearn.impute import SimpleImputer
import dateutil.relativedelta
import multiprocessing as mp
import joblib
import matplotlib.pyplot as plt

def rfstockpicker_backtest(
        df, last_prediction_date_str, rolling_window_years,
        nb_months, n_estimators, n_jobs):
    results={}
    pd.options.mode.chained_assignment = None # Gets rid of a warning
    for i in range(0, nb_months):
        results = {}
        pd.options.mode.chained_assignment = None  # Gets rid of a warning
        d = datetime.datetime.strptime(last_prediction_date_str, "%Y-%m-%d")

        d2 = d - dateutil.relativedelta.relativedelta(months=1+1)
        d4 = d2 - dateutil.relativedelta.relativedelta(years=rolling_window_years)  ##Creating a rolling window interval

        print('d', d, 'd2', d2, 'd4', d4)
        test_set = df.xs(slice(str(d2), str(d)), level='date')
        training_set = df.xs(slice(str(d4), str(d2)), level='date')

        # Labels are the values we want to predict
        train_labels = np.array(training_set['win'])
        # Remove the labels from the features
        columns_to_drop = ['win', 'sprtrn', 'dimension', 'trt1m', 'rtoversp']
        train_features = training_set.drop(columns_to_drop, axis=1)
        # Saving feature names for later use
        train_feature_list = list(train_features.columns)
        # Convert to numpy array
        train_features = np.array(train_features)

        test_labels = np.array(test_set['win'])
        # Remove the labels from the test features
        test_features = test_set.drop(columns_to_drop, axis=1)
        # Convert to numpy array
        test_features = np.array(test_features)

        # Import the model we are using
        from sklearn.ensemble import RandomForestRegressor
        # Instantiate model
        rf = RandomForestRegressor(n_estimators=n_estimators, random_state=42, n_jobs=n_jobs)

        # Train the model on training data
        rf.fit(train_features, train_labels)

        # Use the forest's predict method on the test data
        stock_predictions = rf.predict(test_features)
        result_set = test_set
        result_set['predictions'] = stock_predictions
        results[i] = result_set

        # Get numerical feature importances
        importances = list(rf.feature_importances_)
        # List of tuples with variable and importance
        feature_importances = [(feature, round(importance, 2)) for feature, importance in
                               zip(train_feature_list, importances)]
        # Sort the feature importances by most important first
        feature_importances = sorted(feature_importances, key=lambda x: x[1], reverse=True)

    return results, feature_importances, importances, train_feature_list

results, feature_imp, importances, train_feature_list = rfstockpicker_backtest(df_mrq_pruned,("2018-12-01"),7,1,100,24)

portfolio=results[0].nlargest(10, 'predictions')
print(portfolio)

monthly_yields=[]
SP500_monthly_yields=[]
compounded_portfolio=1
compounded_SP500=1
for i in range(0,len(results)):
    monthly_yields.append((results[i].nlargest(5, 'predictions')).trt1m.mean().round(2))
    SP500_monthly_yields.append(results[i].sprtrn.mean().round(2))
    compounded_portfolio=(compounded_portfolio*(1+(monthly_yields[i]/100)))
    compounded_SP500=compounded_SP500*(1+(SP500_monthly_yields[i]/100))

print('The compounded return of the portfolio over the period is:',round(((compounded_portfolio-1))*100,2),'%')
print('The compounded return of the SP500 over the period is:',round((compounded_SP500-1)*100,2),'%')

# list of x locations for plotting
x_values = list(range(len(importances)))
# Make a bar chart
plt.bar(x_values, importances, orientation = 'vertical')
# Tick labels for x axis
plt.xticks(x_values, train_feature_list, rotation='vertical')
plt.grid(True)
plt.box(True)
plt.rcParams["figure.facecolor"] = "w"
# Axis labels and title
plt.ylabel('Importance'); plt.xlabel('Feature'); plt.title('Feature Importance');

# Lets try removing features with importance value smaller than 0.02
df_importance = pd.DataFrame(feature_imp, columns = ['Feature', 'Importance'])
feature_to_drop=df_importance.Feature[df_importance['Importance']<0.02]
df_mrq_pruned_important = df_mrq_pruned.drop(columns=feature_to_drop)

## We can run the same backtest over one month with the simplified db:
results2,feature_imp2,importances2,train_feature_list2=rfstockpicker_backtest(df_mrq_pruned_important,("2018-12-01"),7,1,100,24)

results3,feature_imp3,importances3,train_feature_list3=rfstockpicker_backtest(df_mrq_pruned,("2018-12-01"),7,(12*15),100,24)

results4,feature_imp4,importances4,train_feature_list4=rfstockpicker_backtest(df_mrq_pruned_important,("2018-12-01"),7,(12*15),100,24)



