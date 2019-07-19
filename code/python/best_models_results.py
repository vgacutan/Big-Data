import utils
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import *
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.svm import NuSVC
from sklearn.svm import LinearSVC

# setup the randoms tate
RANDOM_STATE = 545510477

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


def features():
	'''
    This function generates features and labels..
    '''

	# Extract previous training data.
	X_train, Y_train = utils.get_data_from_svmlight("../data/training/part-r-00000")

	# Extract testing data.
	X_test, Y_test = utils.get_data_from_svmlight("../data/testing/part-r-00000")


	return X_train, Y_train, X_test, Y_test


def best_models_predictions(X_train,Y_train,X_test,Y_test):
	'''
    This function makes predictions for the models shown below.
    '''

	# Extract important features.
	print("Size of Training Dataset (Predictions):" ,X_train.shape)

	best_models = []

	lr_model = LogisticRegression(C=1.0, class_weight=None, dual=False, fit_intercept=True,
			   intercept_scaling=1, max_iter=5, multi_class='ovr', n_jobs=1,
			   penalty='l2', random_state=545510477, solver='liblinear',
		       tol=0.0001, verbose=0, warm_start=False)

	dt_model = DecisionTreeClassifier(class_weight=None, criterion='entropy', max_depth=1,
			   max_features=None, max_leaf_nodes=None, min_samples_leaf=1,
			   min_samples_split=2, min_weight_fraction_leaf=0, presort=False,
			   random_state=545510477, splitter='best')

	svc_model = SVC(C=1.0, cache_size=200, class_weight=None, coef0=0.0,
				decision_function_shape=None, degree=3, gamma=0.1, kernel='linear',
				max_iter=3000, probability=False, random_state=545510477, shrinking=True,
				tol=0.001, verbose=False)

	nuSVC_model = NuSVC(cache_size=200, class_weight=None, coef0=0.0,
		  		  decision_function_shape=None, degree=3, gamma='auto', kernel='linear',
		  		  max_iter=10000, nu=0.5, probability=False, random_state=545510477,
		  		  shrinking=True, tol=0.001, verbose=False)

	linearSVC_model = LinearSVC(C=1.0, class_weight=None, dual=True, fit_intercept=True,
			  		  intercept_scaling=1, loss='squared_hinge', max_iter=3000,
			  		  multi_class='ovr', penalty='l2', random_state=545510477, tol=0.0001,
			  		  verbose=0)

	rf_model =		RandomForestClassifier(bootstrap=True, class_weight=None, criterion='entropy',
					max_depth=5, max_features='auto', max_leaf_nodes=None,
					min_samples_leaf=3, min_samples_split=10,
					min_weight_fraction_leaf=0.0, n_estimators=20, n_jobs=1,
					oob_score=False, random_state=545510477, verbose=0,
					warm_start=False)

	# Store best models in list.
	best_models = [lr_model,dt_model,svc_model,nuSVC_model,linearSVC_model,rf_model]

	names = ["Logistic Regression", "Decision Tree", "Support Vector Machine (SVC)",
			 "Support Vector Machines (NuSVC)", "Support Vector Machines (LinearSVC)", "Random Forests"]

	for idx, m in enumerate(best_models):
		print "Training and Test Results for " + names[idx] + "........."
		print(m)

		# Fit the model with training data.
		m.fit(X_train, Y_train)

		# Make a prediction.
		Y_pred_train = m.predict(X_train)
		Y_pred_test = m.predict(X_test)

		# Display Metrics
		print("Size of Training Labels: " + str(Y_train.shape))
		display_metrics(names[idx] + "(Training)", Y_pred_train, Y_train)
		print("Size of Testing Labels: " + str(Y_test.shape))
		display_metrics(names[idx] + "(Test)", Y_pred_test, Y_test)


def main():

	# LOAD FEATURES
	X_train, Y_train, X_test, Y_test = features()

	print("Using Full Training/Test Sets and Optimized Models and No Feature Selection")
	print("_____________________________________________________________")
	print("Dimensions of Training Set", X_train.shape)
	print("Dimensions of Testing Set", X_test.shape)

	# Metrics Using No Feature Selection
	best_models_predictions(X_train, Y_train, X_test, Y_test)





if __name__ == "__main__":
    main()