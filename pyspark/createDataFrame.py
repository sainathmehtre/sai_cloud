from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master("local").appName("Crate DataFrame").getOrCreate()

    emp_data = [
        (1, "John Doe", "Male", 60000.0, "USA"),
        (2, "Jane Smith", "Female", 55000.0, "Canada"),
        (3, "Alice Johnson", "Female", 65000.0, "UK"),
        (4, "Bob Williams", "Male", 62000.0, "Australia"),
        (5, "Eve Davis", "Female", 70000.0, "India"),
        (6, "Charlie Brown", "Male", 58000.0, "Germany"),
        (7, "Diana Miller", "Female", 60000.0, "France"),
        (8, "Frank Johnson", "Male", 62000.0, "Spain"),
        (9, "Grace Wilson", "Female", 54000.0, "Italy"),
        (10, "Henry Davis", "Male", 68000.0, "Japan"),
        (11, "Isabel Clark", "Female", 59000.0, "Brazil"),
        (12, "Jack Turner", "Male", 63000.0, "Mexico"),
        (13, "Katherine White", "Female", 67000.0, "South Africa"),
        (14, "Louis Harris", "Male", 56000.0, "Russia"),
        (15, "Mia Lee", "Female", 61000.0, "China")
    ]

    emp_columns = ["empId", "empName", "empGender", "empSalary", "empCountry"]

    df = spark.createDataFrame(emp_data, emp_columns)
    df.show()
    df.printSchema()
    df.count()