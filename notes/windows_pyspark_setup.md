# PySpark Local Setup on Windows (Complete Guide)

This guide explains how to set up PySpark locally on Windows using **VS Code** and **Python virtual environments**. It also covers the common errors you may face (like `WinError 2`) and how to fix them.

---

## 1. Prerequisites

* **Python** 3.8 or higher (install from [python.org](https://www.python.org/downloads/))
* **Java JDK** (OpenJDK 11 or later)
* **VS Code** (with Python extension)
* **pip** package manager

---

## 2. Create and Activate Virtual Environment

```bash
cd C:\Users\<your-username>\Documents\noaa-ais-pipeline
python -m venv .venv
.venv\Scripts\activate
```

You should see `(.venv)` before your terminal path.

---

## 3. Install Required Libraries

```bash
pip install pyspark findspark
```

Confirm installation:

```bash
pip show pyspark
```

---

## 4. Install Java (OpenJDK)

1. Download **Temurin OpenJDK** from [https://adoptium.net](https://adoptium.net)
2. Install to:

   ```
   C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot\
   ```
3. Verify installation:

   ```bash
   java -version
   ```

Expected output:

```
openjdk version "21.0.7" 2025-04-15 LTS
OpenJDK Runtime Environment Temurin-21.0.7+6 (build 21.0.7+6-LTS)
OpenJDK 64-Bit Server VM Temurin-21.0.7+6 (build 21.0.7+6-LTS, mixed mode, sharing)
```

---

## 5. Set JAVA_HOME Environment Variable

Run in PowerShell:

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
```

Then verify:

```powershell
echo %JAVA_HOME%
```

Also test the Java binary:

```powershell
& "$env:JAVA_HOME\bin\java.exe" -version
```

---

## 6. Configure Hadoop Binary (Fix for WinError 2)

Spark requires `winutils.exe` even for local mode on Windows.

1. Create directories:

   ```bash
   mkdir C:\hadoop
   mkdir C:\hadoop\bin
   ```
2. Download `winutils.exe` from a trusted Spark/Hadoop build (e.g., [https://github.com/steveloughran/winutils](https://github.com/steveloughran/winutils))
3. Place `winutils.exe` in:

   ```
   C:\hadoop\bin\winutils.exe
   ```
4. Add environment variable:

   ```powershell
   setx HADOOP_HOME "C:\hadoop"
   ```

---

## 7. Working PySpark Initialization Script (Tested)

Create a file named `test.py`:

```python
import os
import findspark
from pyspark.sql import SparkSession

def init_spark():
    try:
        # Explicit environment setup
        os.environ["JAVA_HOME"] = r"C:\\Program Files\\Eclipse Adoptium\\jdk-21.0.7.6-hotspot"
        os.environ["HADOOP_HOME"] = r"C:\\hadoop"
        os.environ["SPARK_HOME"] = r"C:\\Users\\<project location>\\.venv\\Lib\\site-packages\\pyspark"
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

        # Initialize Spark (findspark ensures PySpark finds its jars)
        findspark.init(os.environ["SPARK_HOME"])

        spark = SparkSession.builder \
            .appName("TestPySpark") \
            .master("local[*]") \
            .config("spark.driver.host", "127.0.0.1") \
            .getOrCreate()

        print("Spark initialized successfully")
        print(f"Spark version: {spark.version}")
        return spark

    except Exception as e:
        print(f"Error initializing Spark: {e}")
        return None

if __name__ == '__main__':
    spark = init_spark()
    if spark:
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        df.show()
        spark.stop()
```

Run it using:

```bash
python test.py
```

Expected output:

```
Spark initialized successfully
Spark version: 3.5.1
+---+-----+
| id| name|
+---+-----+
|  1|Alice|
|  2|  Bob|
+---+-----+
```

---

## 8. Common Errors and Fixes

| Error               | Cause                            | Fix                                                        |
| ------------------- | -------------------------------- | ---------------------------------------------------------- |
| `WinError 2`        | Spark can't find Hadoop binaries | Ensure `winutils.exe` exists in `C:\hadoop\bin`            |
| `JAVA_HOME not set` | Environment variable missing     | Run `setx JAVA_HOME "<jdk_path>"` and restart shell        |
| `pyspark not found` | Library missing in venv          | Run `pip install pyspark findspark`                        |
| `Invalid Spark URL` | Localhost hostname issue         | Add `.config("spark.driver.host", "127.0.0.1")` to builder |

---

## 9. Optional: Silence Warnings

To reduce Spark console warnings, add:

```python
spark.sparkContext.setLogLevel("ERROR")
```

---

## 10. Verification Summary

* Python environment active
* Java configured with `JAVA_HOME`
* Hadoop binary present
* PySpark script runs without error
* `spark.driver.host` configured for Windows stability

---

© 2025 Siva Prasath. All rights reserved.
