from time import perf_counter
import pandas as pd
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import joblib
import random
import json
from sklearn.metrics import classification_report
def fit_log_reg():
    col_names = ["attrs","text","ancestors","output_count","avg_tokens",
                 "completeness","html_feature"] +["avg_link_text","link_subpaths","link_destinations","label"]
    # load dataset
    dataset = pd.read_csv("/Users/valentineokundaye/PycharmProjects/Harvest/trinity/logistic_features_small_best4.csv",
                          header=None, names=col_names)
    #split dataset in features and target variable

    #feature_cols = ["attrs","text","ancestors","output_count","avg_tokens","completeness"] + ["avg_link_text","link_subpaths","link_destinations"]
    feature_cols = ["attrs", "output_count", "avg_link_text","link_subpaths","link_destinations"]
    X = dataset[feature_cols] # Features
    y = dataset.label # Target variable


    # split X and y  columns into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=16)

    # import the class
    # instantiate the model (using the default parameters)
    logreg = LogisticRegression(random_state=16,max_iter=1000)
    start = perf_counter()
    # fit the model with data
    #logreg = joblib.load('models/' + 'logistic_regv2.bin')

    logreg.fit(X_train, y_train)


    y_pred = logreg.predict(X_test)
    stop = perf_counter()


    print(f"Number of training samples {len(X_train)} ")
    print(f"Number of test samples {len(y_test)}")

    '''filename = 'models/logistic_smb4V2.bin'
    joblib.dump(logreg, filename)'''


    print(f'finished in {stop-start} seconds')

    con_matrix = metrics.confusion_matrix(y_test, y_pred)
    print(con_matrix)

    true_pos = con_matrix[1][1]
    false_pos = con_matrix[0][1]
    false_neg = con_matrix[1][0]

    precision = true_pos/(true_pos+false_pos)
    recall = true_pos/(true_pos+false_neg)

    print(f"precision: {precision}  recall: {recall}")
    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix = con_matrix, display_labels = [0, 1])
    cm_display.plot()
    plt.show()

def fit_naive_bayes():
    with open('text_samples.json') as f:
        data = json.load(f)
        pos = data['pos']
        neg = data['neg']

        pos_attrs = [i + ' 1' for i in pos['attrs'] if i ]
        neg_attrs = [i + ' 0' for i in neg['attrs'] if i]

        pos_text = [i+ ' 1' for i in pos['text'] if i]
        neg_text = [i + ' 0' for i in  neg['text'] if i]
        random.shuffle(neg_attrs)
        random.shuffle(neg_text)
        neg_text = neg_text[:160]
        neg_attrs = neg_attrs[:160]

        attr_data = neg_attrs + pos_attrs
        text_data = neg_text + pos_text


        # Get the labels at the end of each value
        attr_labels = [int(i[-1]) for i in attr_data]
        text_labels = [int(i[-1]) for i in text_data]

        # Get the data and exclude the labels
        attr_data = [i[:-1].strip() for i in attr_data]
        text_data = [i[:-1].strip() for i in text_data]
        x_train, x_test, y_train, y_test = train_test_split(attr_data, attr_labels, test_size=0.25, random_state=16)

    # Now we define the training set
    print(f"Number of unique classes {len(set(text_labels))}")

    print(f"Number of training samples {len(x_train)} ")
    print(f"Number of test samples {len(y_test)}")

    model = make_pipeline(TfidfVectorizer(), MultinomialNB())
    model.fit(x_train, y_train)


    predicted_categories = model.predict(x_test)
    filename = 'naive_bayes_text_4.bin'
    joblib.dump(model, filename)


    con_matrix = metrics.confusion_matrix(y_test, predicted_categories)
    true_pos = con_matrix[1][1]
    false_pos = con_matrix[0][1]
    false_neg = con_matrix[1][0]

    precision = true_pos / (true_pos + false_pos)
    recall = true_pos / (true_pos + false_neg)
    print(f"precision: {precision}  recall: {recall}")

    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix=con_matrix, display_labels=[0, 1])
    cm_display.plot()
    plt.show()


def fit_naive_bayes_link():
    with open('datasets/link_sample_subpaths.json') as f:
        data = json.load(f)
        pos = data['pos']
        neg = data['neg']

        pos_dest = [i + ' 1' for i in pos['destinations'] if i ]
        neg_dest = [i + ' 0' for i in neg['destinations'] if i]

        pos_subpath = [i+ ' 1' for i in pos['subpaths'] if i]
        neg_subpath = [i + ' 0' for i in  neg['subpaths'] if i]
        '''random.shuffle(neg_dest)
        random.shuffle(neg_subpath)'''
        neg_subpath = neg_subpath[:150]
        neg_dest = neg_dest[:160]

        dest_data = neg_dest + pos_dest
        sub_data = neg_subpath + pos_subpath


        # Get the labels at the end of each value
        dest_labels = [int(i[-1]) for i in dest_data]
        sub_labels = [int(i[-1]) for i in sub_data]

        # Get the data and exclude the labels
        dest_data = [i[:-1].strip() for i in dest_data]
        sub_data = [i[:-1].strip() for i in sub_data]
        x_train, x_test, y_train, y_test = train_test_split(sub_data, sub_labels, test_size=0.25, random_state=16)

    # Now we define the training set
    print(f"Number of unique classes {len(set(sub_labels))}")

    print(f"Number of training samples {len(x_train)} ")
    print(f"Number of test samples {len(y_test)}")

    model = make_pipeline(TfidfVectorizer(), MultinomialNB())
    model.fit(x_train, y_train)


    predicted_categories = model.predict(x_test)
    filename = 'naive_bayes_sub.bin'
    joblib.dump(model, filename)


    con_matrix = metrics.confusion_matrix(y_test, predicted_categories)
    true_pos = con_matrix[1][1]
    false_pos = con_matrix[0][1]
    false_neg = con_matrix[1][0]

    precision = true_pos / (true_pos + false_pos)
    recall = true_pos / (true_pos + false_neg)
    print(f"precision: {precision}  recall: {recall}")

    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix=con_matrix, display_labels=[0, 1])
    cm_display.plot()
    plt.show()
