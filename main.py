import pandas as pd
import dask.dataframe as dd
import time
import matplotlib.pyplot as plt

n_processors = [10, 20]
n_processors_time = {}

for processor in n_processors:
    start_time = time.time()

    #TASK A
    #Loading the data of datasets
    df_distance = dd.read_csv("Trips_by_Distance.csv")
    df_full = dd.read_csv("Trips_Full Data.csv")

    #Interpret unspecified integer columns as floats because of error
    df_distance = dd.read_csv("Trips_by_Distance.csv", assume_missing=True)

    #How many people are staying at home? How far are people traveling when they don’t stay home?
    uniqueWeeks = df_distance["Week"].nunique().compute()

    #Number of people staying home per week
    peopleAtHome = df_distance.groupby(by = "Week")["Population Staying at Home"].mean().compute()

    #Average Distance for all of these columns
    distanceOfPeopleTraveled = df_distance[
        [
            "Number of Trips 1-3",
            "Number of Trips 3-5",
            "Number of Trips 5-10",
            "Number of Trips 10-25",
            "Number of Trips 25-50",
            "Number of Trips 50-100",
            "Number of Trips 100-250",
            "Number of Trips 250-500",
            "Number of Trips >=500",
        ]
    ].mean().compute()

    #Same thing but for the Trips_Full Data.csv
    distanceOfPeopleTraveled2 = df_full[
        [
            "Trips 1-3 Miles",
            "Trips 3-5 Miles",
            "Trips 5-10 Miles",
            "Trips 10-25 Miles",
            "Trips 25-50 Miles",
            "Trips 50-100 Miles",
            "Trips 100-250 Miles",
            "Trips 250-500 Miles",
            "Trips 500+ Miles",
        ]
    ].mean().compute()

    #Print Statements
    print("")
    print(f"Total number of weeks: {uniqueWeeks -1}")
    print("")
    print(f"Average number of people staying at home per week: {peopleAtHome}")
    print("")
    print("Average distance traveled by people when they don't stay home per week: \n",distanceOfPeopleTraveled)

    #TASK B
    #FILTERING ROWS WHERE POPULATION IS 10 MILLION AND 10-25 TRIPS
    df_filtered10_25 = df_distance[(df_distance["Population Staying at Home"] > 10000000) & (df_distance["Number of Trips 10-25"])]
    date10_25 = df_filtered10_25["Date"].compute()

    #Filtering the rows where population is 10 million and 50-100 trips
    df_filtered50_100 = df_distance[(df_distance["Population Staying at Home"] > 10000000) & (df_distance["Number of Trips 50-100"])]
    date50_100 = df_filtered50_100["Date"].compute()

    print("")
    print(f"Dates where more than 10,000,000 people conducted 10-25 trips:")
    print(date10_25)
    print("")
    print(f"Dates where more than 10,000,000 people conducted 50-100 trips:")
    print(date50_100)

    #TASK E
    #Average number of travelers by distance-trips
    print("")
    print("Average Number of travelers: \n", distanceOfPeopleTraveled2)

    #Average Traveler Bar Chart
    plt.figure(figsize = (8, 6))
    #For type of chart, and colour
    distanceOfPeopleTraveled2.plot(kind="bar", color="lime")
    plt.title("Average number of Travelers by distance-trips")
    plt.ylabel("Number of participants of Travelers")
    plt.xlabel("Distance-trips")
    plt.grid(axis="y", linestyle = "-", alpha=1)
    plt.show()

    dask_time = time.time()-start_time
    n_processors_time[processor]= dask_time

print("")
print("Printing the processor execution time")
print(n_processors_time)

#The plots for when processor = 10,20 comparison 
plt.figure(figsize = (8, 6))
plt.bar(n_processors_time.keys(), n_processors_time.values(), color="black")
plt.title("Comparison of Execution Time with Different Number of Processors")
plt.ylabel("Execution time in seconds")
plt.xlabel("Number of Processors")
plt.grid(axis = "y", linestyle = "-", alpha = 1)
plt.show()


