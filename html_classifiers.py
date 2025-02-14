import numpy as np, pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('TkAgg')
import joblib
sns.set() # use seaborn plotting style


class NaiveBayes:
    def __init__(self):
        self.text_model = joblib.load('models/'+'naive_bayes_text_2.bin')#joblib.load('models/'+'naive_bayes_text.bin')
        self.attribute_model = joblib.load('models/'+'naive_bayes_attrs_2.bin')#joblib.load('models/'+'naive_bayes_attrs.bin')

        self.link_subpath_model = joblib.load(
            'models/' + 'naive_bayes_sub.bin')  # joblib.load('models/'+'naive_bayes_text.bin')
        self.link_destination_model = joblib.load(
            'models/' + 'naive_bayes_dest.bin')  # joblib.load('models/'+'naive_bayes_attrs.bin')

    def classify_attr_tokens(self,input):
        if not input:
            return 0
        output = self.attribute_model.predict(input)
        return output[0]

    def classify_text_tokens(self,input):
        if not input:
            return 0
        output = self.text_model.predict(input)

        return output[0]
    def classify_subpaths(self, input):
            if not input:
                return 0
            output = self.link_subpath_model.predict(input)
            return output[0]
    def classify_destination_slug(self, input):
            if not input:
                return 0
            output = self.link_destination_model.predict(input)
            return output[0]


class LogReg:
    def __init__(self):
        self.model = joblib.load('models/' + 'logistic_sm_b4.bin')
        #self.model = joblib.load('models/' + 'logistic_regv2.bin')
    def make_prediction(self,column_names, column_values):

        #prep feature list for model
        features = [[i] for i in column_values]
        combine= dict(zip(column_names, features))
        full_feature  = pd.DataFrame(combine)

        output = self.model.predict(full_feature)

        return output[0]
