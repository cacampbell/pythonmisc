#!/usr/bin/env Rscript
filesizes <- read.table("filesizes", header=FALSE, sep='\t')
filesizes$name <- filesizes[,1]
filesizes$read1 <- filesizes[,2]
filesizes$read2 <- filesizes[,3]
filesizes$mapped <- filesizes[,4]
quantiles <- quantile(filesizes$mapped)
lowerq <- quantiles[2]
upperq <- quantiles[4]
iqr <- IQR(filesizes$mapped)
mild.threshold.upper <- (iqr * 1.5) + upperq
mild.threshold.lower <- lowerq - (iqr * 1.5)
extreme.threshold.upper <- (iqr * 3) + upperq
extreme.threshold.lower <- lowerq - (iqr * 3)
outliers.minor <- filesizes$name[filesizes$mapped > mild.threshold.upper | filesizes$mapped < mild.threshold.lower]
outliers.extreme <- filesizes$name[filesizes$mapped > extreme.threshold.upper | filesizes$mapped < extreme.threshold.lower]
cat("Minor Outliers:", sprintf("%s", outliers.minor), "\n", sep=" ")
cat("Extreme Outliers:", sprintf("%s", outliers.extreme), "\n", sep=" ")

library(scatterplot3d)
png("3D_Filesizes.png", width=1000, height=1000)
scatterplot3d(filesizes$read1, filesizes$read2, filesizes$mapped, main="Filesizes")
filesizes$pe <- filesizes$read1 + filesizes$read2
png("2D_Filesizes.png", width=1000, height=1000)
plot(filesizes$pe, filesizes$mapped)
