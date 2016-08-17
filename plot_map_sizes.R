#!/usr/bin/env Rscript
filesizes <- read.table("filesizes", header=FALSE, sep='\t')
filesizes$name <- c(filesizes$V1)
filesizes$read1 <- c(filesizes$V2)
filesizes$read2 <- c(filesizes$V3)
filesizes$mapped <- c(filesizes$V4)
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
cat(length(outliers.minor), " Minor Outliers: ", outliers.minor, "\n")
cat(length(outliers.extreme), " Extreme Outliers: ", outliers.extreme, "\n")

library(scatterplot3d)
png("3D_Filesizes.png", width=1000, height=1000)
scatterplot3d(filesizes$read1, filesizes$read2, filesizes$mapped, main="Filesizes")
dev.off()
filesizes$pe <- filesizes$read1 + filesizes$read2
png("2D_Filesizes.png", width=1000, height=1000)
plot(filesizes$pe, filesizes$mapped)
dev.off()
