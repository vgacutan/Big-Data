import utils
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import *
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import make_scorer
import numpy as np
from sklearn.cross_validation import cross_val_score
import matplotlib.pyplot as plt
from best_models_results import best_models_predictions

# setup the randoms tate
RANDOM_STATE = 545510477


def load_SVMLight_data(train_data,test_data):
	'''
    This function loads training and testing data in SVMLight.
    '''

	# Extract previous training data.
	X_train, Y_train = utils.get_data_from_svmlight(train_data)

	# Extract testing data.
	X_test, Y_test = utils.get_data_from_svmlight(test_data)


	return X_train, Y_train, X_test, Y_test


def feature_importance(X_train,Y_train):
	"""
	This function finds important features using an ensemble of decision trees as metric.
	"""

	print("This is the Random State: ", str(RANDOM_STATE))
	clf = ExtraTreesClassifier(random_state=RANDOM_STATE)
	clf = clf.fit(X_train, Y_train)
	print("Min Importance Value:" + str(np.min(clf.feature_importances_)))
	print("Max Importance Value:" + str(np.max(clf.feature_importances_)))
	print("Mean Importance Value:" + str(np.mean(clf.feature_importances_)))

	return clf


def best_model():
	'''
    This function holds our best model.
    '''

	# Selected model using find_model with GridSearch using Cross-Validation 3.
	model = DecisionTreeClassifier(class_weight=None, criterion='entropy', max_depth=1,
            max_features=None, max_leaf_nodes=None, min_samples_leaf=1,
            min_samples_split=2, min_weight_fraction_leaf=0, presort=False,
            random_state=545510477, splitter='best')

	return model


def find_threshold(clf,Xtrain,Y_train):
	# Make an appropriate scoring function
	scoring_function = make_scorer(roc_auc_score, greater_is_better=True)

	# Initialize list of AUCs.
	aucs = []
	nfeatures = []
	thresholds = np.unique(clf.feature_importances_)

	# Find unique feature importance thresholds.
	for i in np.unique(clf.feature_importances_):

		print("Current Feature Importance Value Tested: ", str(i))
		# Reduce features with input threshold.
		reduced = SelectFromModel(clf, i, prefit=True)
		X_new = reduced.transform(Xtrain)
		print("Current Size of Training Features: ", str(X_new.shape))

		# Test on chosen model using reduced features.
		model = best_model()

		cv_scores = cross_val_score(model,X_new,Y_train, scoring=scoring_function,cv=3)
		aucs.append(cv_scores.mean())
		nfeatures.append(X_new.shape[1])
		print("AUC: %0.2f (+/- %0.2f)" % (cv_scores.mean(), cv_scores.std() * 2))


	return aucs, nfeatures, thresholds


def best_features(clf,Xtrain, Xtest,aucs_scores,thresholds):

	# Find best reduced feature set.
	best_i = len(aucs_scores[::-1]) - np.argmax(aucs_scores[::-1]) - 1
	best_threshold = thresholds[best_i]

	# Reduce feature set.
	reduced = SelectFromModel(clf,best_threshold, prefit=True)
	X_train_reduced = reduced.transform(Xtrain)
	X_test_reduced = reduced.transform(Xtest)
	print("New Training Set Size: ", str(X_train_reduced.shape))
	print("New Test Set Size: ", str(X_test_reduced.shape))
	print("Selection Threshold: ", str(best_threshold))


	return X_train_reduced, X_test_reduced,  reduced.threshold_


def classification_metrics(Y_pred, Y_true):

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


def num_features_graph(n_features,auc_scores):
	# Construct graph.
	plt.figure()
	plt.title("AUC Cross-Validation Scores vs. Number of Features")
	plt.xlabel("Number of Features")
	plt.ylabel("AUC CV Score")
	plt.xlim(30,np.min(n_features))

	# Plot AUC scores
	output = '../data/auc_feature_reduction.png'
	print("Outputing:",output)
	plt.plot(n_features, auc_scores, 'o-', color="r", label="Decision Tree")
	plt.savefig(output)

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

	# LOAD FEATURES
	train_data = "../data/training/part-r-00000"
	test_data = "../data/testing/part-r-00000"
	X_train, Y_train, X_test, Y_test = load_SVMLight_data(train_data,test_data)

	print("Number of Patients:", str(X_train.shape[0] + X_test.shape[0]))

	# COMPUTE FEATURE IMPORTANCE
	clf = feature_importance(X_train, Y_train)

	# FIND THE THRESHOLD TO SELECT FEATURE
	auc_scores, n_features, thresholds = find_threshold(clf,X_train,Y_train)

	# GENERATE GRAPH OF RESULTS
	num_features_graph(n_features, auc_scores)

	# TEST RESULTS
	X_train_reduced, X_test_reduced, best_threshold = best_features(clf,X_train,X_test,auc_scores,thresholds)
	print(best_threshold)

	print("Using Full Training and Testing Set on Optimized Models with Feature Selection")
	best_models_predictions(X_train_reduced, Y_train, X_test_reduced, Y_test)

	# RETURN MOST IMPORTANT FEATURES

	f_importances = clf.feature_importances_
	causes_idx = np.where(f_importances >= 0.0256667359488) # Force 9, even if really needs 1.

	print("Best Features Importance Values")
	print(f_importances[causes_idx])

	print("9 of Most Important Parkinson Features:")
	print(causes_idx)











if __name__ == "__main__":
    main()