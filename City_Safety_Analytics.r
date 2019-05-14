# Databricks notebook source
# DBTITLE 1,Libraries
library(SparkR)
library(magrittr)
install.packages("geosphere")
library(geosphere)
install.packages("reldist")
library(reldist)
library(dplyr)
library(ggplot2)
library(gridExtra)


# COMMAND ----------

# DBTITLE 1,Load Data
# initialize session
sparkR.session(
  sparkConfig = list(
    'fs.azure.sas.citydatacontainer.azureopendatastorage.blob.core.windows.net' = '?st=2019-02-26T02%3A34%3A32Z&se=2119-02-27T02%3A34%3A00Z&sp=rl&sv=2018-03-28&sr=c&sig=XlJVWA7fMXCSxCKqJm8psMOh0W4h7cSYO28coRqF2fs%3D'
  )
)

# load safety data
wasbs_path_safety <- 'wasbs://citydatacontainer@azureopendatastorage.blob.core.windows.net/Safety/Release/'

# full data
safetyDF <- read.df(wasbs_path_safety, source = "parquet")


# COMMAND ----------

# DBTITLE 1,Explore Data
# cities available: Chicago, Seattle, New York, Boston, San Francisco
# note: no spaces in city names in data - e.g., NewYork, SanFrancisco
cities = c("Chicago","Seattle","New York","Boston","San Francisco")
cities.data = c("Chicago","Seattle","NewYork","Boston","SanFrancisco")

city.use = "Chicago"
safety.city = SparkR::filter(safetyDF, safetyDF$city == city.use)
display(safety.city)

# COMMAND ----------

# DBTITLE 1,Top and Bottom Safety Issues Per City
# select city to use
city.use = "Chicago"
safety.city = SparkR::filter(safetyDF, safetyDF$city == city.use)
safety.city.df = SparkR::collect(safety.city)

# make table of top and bottom most frequent safety issues
safety.top = table(safety.city.df$category) # use the 'category' column for Seattle, Chicago and 'subcategory' for San Francisco, Boston
safety.top.df = data.frame(Category = names(safety.top), Count = as.numeric(safety.top))
levels = safety.top.df$Category[order(safety.top.df$Count, decreasing = T)]
safety.top.df$Category = factor(safety.top.df$Category, levels = levels)
safety.top.df = safety.top.df[order(safety.top.df$Count, decreasing = T),]


# plot top and bottom most frequent
num.rows = 30
#safety.top.df = safety.top.df[5:nrow(safety.top.df),] # optional: remove top n items if they are too general/dominant
rows.to.plot = c(1:num.rows,(nrow(safety.top.df) - num.rows):nrow(safety.top.df))

ggplot(safety.top.df[rows.to.plot,], aes(x = Category, y = Count, fill = Count)) + geom_col() +
  labs(y = "Count of Incidents", title = paste0("Safety Incidents, ", city.use)) +
  theme(axis.title = element_text(size = 16), title = element_text(size = 16), axis.text.y = element_text(size = 6), legend.position = "none") +
  scale_fill_gradient(low = "blue", high = "red") +
  coord_flip()

# COMMAND ----------

# DBTITLE 1,Changes Over Time - Volume of All Safety Calls
# add year column for easy yearly aggregates
safetyDF$Year = year(safetyDF$dateTime)

safetyDF.Year = SparkR::summarize(groupBy(safetyDF, safetyDF$Year, safetyDF$City), Count = SparkR::n(safetyDF$Year))

safety.df.year = SparkR::collect(safetyDF.Year)
ggplot(safety.df.year, aes(x = Year, y = Count, color = City)) + geom_line() + geom_point() +
  labs(y = "Yearly Count of All Safety Calls")


# COMMAND ----------

# DBTITLE 1,Changes Over Time - Volume of Specific Safety Calls
# add year column for easy yearly aggregates
safetyDF$Year = year(safetyDF$dateTime)

# specify safety keyword
safety.use = "graffiti"

safetyDF$subcategory = lower(safetyDF$subcategory)
safetyDF$category = lower(safetyDF$category)

createOrReplaceTempView(safetyDF, "safetydf")
sql.str = paste0("SELECT * FROM safetydf WHERE subcategory rlike '", safety.use,"' or category rlike '", safety.use,"'")
safetyDF.Single = SparkR::sql(sql.str)

safetyDF.Single.Year = SparkR::summarize(groupBy(safetyDF.Single, safetyDF.Single$Year, safetyDF.Single$City), Count = SparkR::n(safetyDF.Single$Year))

safety.df.single.year = SparkR::collect(safetyDF.Single.Year)
ggplot(safety.df.single.year, aes(x = Year, y = Count, color = City)) + geom_line() + geom_point() +
  labs(title = paste0("Yearly count of ",safety.use," calls"))

# COMMAND ----------

# DBTITLE 1,Geo-cluster for Specified Safety Type in Each City
### set parameters for data selection ###


# set sample size
num.samples = 5000

# select safety type
safety.use = "pothole"

### cluster crimes in each city ###

# list to store results
location.clusters.ls = list()

# data clean-up for processing
safetyDF$subcategory = lower(safetyDF$subcategory)
safetyDF$category = lower(safetyDF$category)

# get geo-clusters for each city based on specified safety type
for(c in 1:length(cities)) {
  
  createOrReplaceTempView(safetyDF, "safetydf")
  sql.str = paste0("SELECT * FROM safetydf WHERE subcategory rlike '",safety.use,"' or category rlike '",safety.use,"' and City = '",cities.data[c],"'")
  safety.df.single = SparkR::sql(sql.str)

  # clean up resulting data to remove NAs and records with invalid longitudes
  safety.df.single = safety.df.single %>%
    SparkR::filter(isNotNull(safety.df.single$longitude)) %>%
    SparkR::filter(safety.df.single$longitude < 360) %>%
    SparkR::filter(safety.df.single$longitude > -360)

  # run clustering over data subset
  # randomly sample X number of crimes
  safety.df = SparkR::collect(safety.df.single)
  sample.rows = base::sample(1:nrow(safety.df),num.samples)
  safety.df = safety.df[sample.rows,]
  
  safety.df$Longitude = as.numeric(safety.df$longitude)
  safety.df$Latitude = as.numeric(safety.df$latitude)

  lat.long.mat = as.matrix(safety.df[,c("Longitude","Latitude")])
  safety.dist = distm(lat.long.mat, fun=distHaversine)

  safety.clust <- hclust(as.dist(safety.dist), method="complete")

  # define the distance threshold
  distance.threshold=1000

  # define clusters based on a tree "height" cutoff "d" and add them to the SpDataFrame
  safety.clusters <- cutree(safety.clust, h=distance.threshold)

  location.clusters = data.frame(Cluster = safety.clusters, Latitude = safety.df$latitude,Longitude = safety.df$longitude)

  location.clusters.ls[[c]] = location.clusters
  
}




# COMMAND ----------

# DBTITLE 1,Plot Distributions of Safety in Geo-Clusters
cities.display.names = c("San Francisco","Chicago","New York","Boston")

plots.ls = list()
ginis = rep(0, length(location.clusters.ls))

for(c in 1:length(location.clusters.ls)) {
  location.cluster.use = location.clusters.ls[[c]]
  
  ### get the number of clusters with each number of crimes
  cluster.counts.df = data.frame(Cluster = names(table(location.cluster.use$Cluster)), Count = as.numeric(table(location.cluster.use$Cluster)))

  max.count = max(cluster.counts.df$Count)
  num.clusters = rep(0,max.count)
  for(i in 1:max(cluster.counts.df$Count)) {
    num.clusters[i] = length(cluster.counts.df$Count[cluster.counts.df$Count == i])
  }

  ### plot
  plot.df = data.frame(Safety.Cluster = num.clusters, Safety = 1:length(num.clusters))

  g = round(gini(plot.df$Safety.Cluster),2)
  ginis[c] = g

  plots.ls[[c]] = ggplot2::ggplot(plot.df, ggplot2::aes(x = Safety, y = Safety.Cluster)) + ggplot2::geom_bar(stat = "identity", fill = "light blue", color = "black") +
    ggplot2::labs(x = "Number of Safety Calls", y = "Number of Clusters", title = paste0(cities[c]," ",safety.use," calls, Gini = ",g)) +
    ggplot2::theme(axis.text = ggplot2::element_text(size = 14), axis.title = ggplot2::element_text(size = 16), title = ggplot2::element_text(size = 10))

}

gridExtra::grid.arrange(plots.ls[[1]], plots.ls[[2]], plots.ls[[3]], plots.ls[[4]], ncol = 2)


# COMMAND ----------


