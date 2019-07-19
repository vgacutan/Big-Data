from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import *

import utils

# setup the randoms tate
RANDOM_STATE = 545510477


def logistic_regression_pred(X_train, Y_train):
	# train a logistic regression classifier using X_train and Y_train. Use this to predict labels of X_train
	# use default params for the classifier

	# Create a Logistic Regression classifier.
	logreg = LogisticRegression(random_state=RANDOM_STATE)

	# Fit the model with training data.
	logreg.fit(X_train,Y_train)

	# Make a prediction.
	Y_pred = logreg.predict(X_train)

	return Y_pred


def svm_pred(X_train, Y_train):
	# train a SVM classifier using X_train and Y_train. Use this to predict labels of X_train
	# use default params for the classifier

	# Create a Support Vector Machine classifier.
	linearSVM = LinearSVC(random_state=RANDOM_STATE)

	# Fit the model with training data.
	linearSVM.fit(X_train,Y_train)

	# Make a prediction.
	Y_pred = linearSVM.predict(X_train)

	return Y_pred


def decisionTree_pred(X_train, Y_train):
	# train a decision tree classifier using X_train and Y_train. Use this to predict labels of X_train
	# use max_depth as 5

	# Create a Decision Tree classifier.
	dt = DecisionTreeClassifier(max_depth=5, random_state=RANDOM_STATE)

	# Fi the model with training data.
	dt.fit(X_train,Y_train)

	# Make a prediction.
	Y_pred = dt.predict(X_train)

	return Y_pred


def classification_metrics(Y_pred, Y_true):
	# NOTE: It is important to provide the output in the same order

	# Compute accuracy score.
	accuracy = accuracy_score(Y_true, Y_pred)

	# Compute AUC score.
	auc = roc_auc_score(Y_true,Y_pred)

	# Compute precision score.
	precision = precision_score(Y_true,Y_pred)

	# Compute recall score
	recall = recall_score(Y_true,Y_pred)

	# Compute F1 Score
	f1 = f1_score(Y_true,Y_pred)


	return accuracy, auc, precision, recall, f1

# input: Name of classifier, predicted labels, actual labels
def display_metrics(classifierName,Y_pred,Y_true):
	print "______________________________________________"
	print "Classifier: "+classifierName
	acc, auc_, precision, recall, f1score = classification_metrics(Y_pred,Y_true)
	print "Accuracy: "+str(acc)
	print "AUC: "+str(auc_)
	print "Precision: "+str(precision)
	print "Recall: "+str(recall)
	print "F1-score: "+str(f1score)
	print "______________________________________________"
	print ""

def main():
	X_train, Y_train = utils.get_data_from_svmlight("../data/training/part-r-00000")

	print("Baseline Using Full Training Set and No Parameter Optimization")
	print("_____________________________________________________________")
	print("Dimensions of Training Set", X_train.shape)
	display_metrics("Logistic Regression",logistic_regression_pred(X_train,Y_train),Y_train)
	display_metrics("SVM",svm_pred(X_train,Y_train),Y_train)
	display_metrics("Decision Tree",decisionTree_pred(X_train,Y_train),Y_train)
	

if __name__ == "__main__":
	main()
	
