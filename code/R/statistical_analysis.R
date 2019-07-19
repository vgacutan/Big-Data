# IN from FeatureSelection.py 

#Best Features Imortance Values
#[ 0.07746535  0.05020073  0.0279179   0.06573103  0.06717689  0.03175642
#  0.06549024  0.02566674  0.02698589]

#The Most Important Parkinson Features:
#  (array([2038, 2042, 2044, 2052, 2057, 2066, 2067, 2068, 2164], dtype=int64),)

# Import libraries
library(reshape2)
library(ggplot2)

# Helper functions

accuracy <- function(labels,labels_pred){
  
  # This function calculates the accuracy between the labels and the predicted labels.
  
  # Find common rows between true labels and predicted labels.
  acc <- as.double(length(which(labels==labels_pred))) / as.double(length(labels))
  
  return(acc)
  
}

# Set working directory
setwd("K:/bdh/bigdata-bootcamp/parkinsons/code")

# Read text file
rawFeatures <- read.csv("../data/labeled_raw_features/part-r-00000",header=FALSE)

# Rename columns.
names(rawFeatures) <- c("PATNO","IDX","TESTVALUE", "LABEL")

# IDX to Feature Name
idx2featureName <- read.csv("../data/features/part-m-00000",header=FALSE)

# Nename columns.
names(idx2featureName) <- c("IDX","FEATURE_NAME")

# Join named with unnamed features.
renamedFeatures <- merge(rawFeatures,idx2featureName, by = "IDX")

# Create data frame of important features and their importance values.
importantFeatures <- data.frame(IDX= c(2038, 2042, 2044, 2052, 2057, 2066, 2067, 2068, 2164),
                                FEATURE_IMPORTANCE= c(0.07746535,0.05020073,0.0279179,0.06573103,0.06717689,0.03175642,0.06549024,0.02566674
                                , 0.02698589))
importantFeatures <-  merge(importantFeatures,idx2featureName, by = "IDX")

# Order by Feature Importance
importantFeatures <- importantFeatures[order(-importantFeatures$FEATURE_IMPORTANCE),]

# Create subset with important features (TALL).
tallPdf <- subset(renamedFeatures, IDX %in% c(importantFeatures$IDX))

# Convert from TALL to WIDE
widePdf <- dcast(tallPdf, PATNO + LABEL ~ FEATURE_NAME, value.var="TESTVALUE")

# Separate between control and PD.
control <- subset(widePdf[widePdf$LABEL==0,], select = -c(PATNO,LABEL))
PD <- subset(widePdf[widePdf$LABEL==1,], select = -c(PATNO,LABEL))

# Feature Statistics

print("Control Statistics")
summary(control)

print("PD Statistics")
summary(PD)

# Generate Box Plots

tallPdf$LABEL <- ifelse(tallPdf$LABEL==0, "Control","PD")
ggplot(tallPdf, aes(x= LABEL, y=TESTVALUE, fill=LABEL)) + geom_boxplot() + facet_wrap (~ FEATURE_NAME, scales = "free") + xlab("Features") + ylab("Test Value") + ggtitle("Comparison of Value Ranges for Important Features Selected")
output <- "../data/parkinson_f_select_boxplots.png"
ggsave(output)
cat("Image Graph Ouput: ", getwd(), output, "\n")

# Rule based

acc <- numeric()

for (name in importantFeatures$FEATURE_NAME){
    print(name)
    widePdf$PREDICTED <- 0
    widePdf$PREDICTED <- ifelse(subset(widePdf, select=name) > 0, 1,0)
    accuracyR <- accuracy(widePdf$PREDICTED,widePdf$LABEL)
    acc <- c(acc,accuracyR)
    print(accuracyR)
}

importantFeatures$ACCURACY <- acc
print(importantFeatures)

# Generate Scatter Plot of Feature That separate the Data.

widePdf$LABEL <- ifelse(widePdf$LABEL==0, "Control","PD")
ggplot(widePdf, aes(x = NHY, y = NP3FTAPL, color=LABEL)) + geom_point() + scale_color_manual(values=c("green","red")) + ggtitle("Comparison of Value Ranges for Important Features Selected")
output <- "../data/parkinson_data_separate.png"
ggsave(output)
cat("Image Graph Ouput: ", getwd(), "/", output)

# Rule Based Search (EXPERIMENTAL)

#widePdf2 <- subset(widePdf2, select=c("PATNO", "LABEL","NHY","NP3RIGLL","NP3PRSPL"))
#cols <- ifelse(subset(widePdf2, select=c("NHY","NP3RIGLL","NP3PRSPL"))>0,1,0)
#s <- ifelse(rowSums(cols) > 2, 1,0)
#widePdf2$PREDICTED <- unlist(as.numeric(s))
#widePdf2$PREDICTED <- 0
#widePdf2$PREDICTED <- rowSums(subset(widePdf2, select=c("NHY","NP3FTAPL","NP3GAIT","NP3PRSPL","NP3RIGLL","NP3RTARU", "NP3RTCON", "NP3SPCH", "PN3RIGRL")))

#widePdf2$PREDICTED <- ifelse(widePdf2$PREDICTED> 4,1,0)
#accuracyR <- accuracy(widePdf2$PREDICTED,widePdf2$LABEL)
#print(accuracyR)

#notClassified <- widePdf2[widePdf2$LABEL!=widePdf2$PREDICTED,]
#print(notClassified[order(-notClassified$LABEL),])


