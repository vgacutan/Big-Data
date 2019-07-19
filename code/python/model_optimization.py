import utils
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import *
from sklearn import grid_search
from sklearn.metrics import make_scorer
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.svm import NuSVC
from sklearn.svm import LinearSVC

# setup the randoms tate
RANDOM_STATE = 545510477


def features():
	'''
    This function generates features.
    '''

	# Extract previous training data.
	X_train, Y_train = utils.get_data_from_svmlight("../data/training/part-r-00000")

	# Extract testing data.
	X_test, Y_test = utils.get_data_from_svmlight("../data/testing/part-r-00000")


	return X_train, Y_train, X_test, Y_test


def find_model(X_train,Y_train):
	'''
    This find the best model on the data using GridSearch and Cross-Validation.
    '''

	print("Size of Training Dataset (Find Model):", X_train.shape)

	# Initialize lists

	# Initialize models
	logreg = LogisticRegression(random_state=RANDOM_STATE)
	dt = DecisionTreeClassifier(random_state=RANDOM_STATE)
	svm_svc = SVC(random_state=RANDOM_STATE)
	svm_Nu = NuSVC(random_state=RANDOM_STATE)
	svm_lin = LinearSVC(random_state=RANDOM_STATE)
	rf = RandomForestClassifier(random_state=RANDOM_STATE)

	models = [logreg, dt, svm_svc, svm_Nu, svm_lin, rf] #knn,

	# Set up parameters we wish to tune.
	parameters_logreg = {'max_iter': (1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000)}
	parameters_dt = {'criterion': ('entropy', 'gini'), 'min_samples_leaf': (1, 2, 3, 4, 5),
					 'min_samples_split': (2, 10, 20, 30),
					 'min_weight_fraction_leaf': (0, 0.1, 0.2),
					 'max_depth': (1, 2, 3, 5, 6, 7, 10)
					  }
	parameters_svm_svc = {'kernel': ('linear', 'sigmoid', 'poly', 'rbf'),
						  'max_iter': (3000, 5000, 10000, 20000, 40000, 100000),
						  'gamma': (0.1, 0.3, 0.5)}
	parameters_svm_Nu = {'kernel': ('linear', 'sigmoid', 'poly', 'rbf'),
						 'max_iter': (10000, 20000, 40000, 100000)}
	parameters_svm_lin = {'max_iter': (3000, 5000, 10000, 20000, 40000, 100000)}
	parameters_rf = {'n_estimators': (1, 5, 7, 10, 20), 'criterion': ('entropy', 'gini'),
					 'min_samples_leaf': (1, 2, 3,),
					 'min_samples_split': (2, 4, 10, 20), 'max_depth': (1, 2, 3, 5, 6, 7, 10)}
	parameters = [parameters_logreg, parameters_dt, parameters_svm_svc, parameters_svm_Nu, parameters_svm_lin,
				  parameters_rf]

	# Create model names

	names = ["Logistic Regression", "Decision Tree", "Support Vector Machine (SVC)",
			 "Support Vector Machines (NuSVC)", "Support Vector Machines (LinearSVC)", "Random Forests"]

	# Make a scoring function
	scoring_function = make_scorer(roc_auc_score)

	for idx, m in enumerate(models):
		print "Starting GridSearchCV with " + names[idx] + "........."

		# Make the GridSearchCV object.
		grid = grid_search.GridSearchCV(m, parameters[idx], cv=3, scoring=scoring_function)

		# Fit Grid Search object.
		grid.fit(X_train, Y_train)

		print "Results for: " + names[idx]
		print "________________________________________________"
		print grid.best_estimator_
		print "AUC: " + str(grid.best_score_) + "\n"

def main():

	print("Model Selection (Using Cross Validation (3 folds) from Full Training Set and No Feature Selection)")

	# LOAD FEATURES
	X_train, Y_train, X_test, Y_test = features()

	print("Number of Patients:", str(X_train.shape[0] + X_test.shape[0]))

	# SHOW BEST PARAMETERS AND RESULTS FOR TESTED MODELS.
	find_model(X_train,Y_train)


if __name__ == "__main__":
    main()