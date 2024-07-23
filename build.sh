#!/bin/sh

# Function to check Java version
check_java() {
  if command -v java > /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | awk -F[\"_] 'NR==1 {print $2}')
    JAVA_MAJOR_VERSION=$(echo $JAVA_VERSION | awk -F[.] '{print $1}')
    if [ "$JAVA_MAJOR_VERSION" -ge 17 ]; then
      echo "Java version $JAVA_VERSION is installed."
    else
      echo "Java 17 or higher is required. Please install the required version of Java."
      exit 1
    fi
  else
    echo "Java is not installed. Please install Java 17 or higher."
    exit 1
  fi
}

# Function to check Maven version
check_maven() {
  if command -v mvn > /dev/null; then
    MAVEN_VERSION=$(mvn -version 2>&1 | awk '/Apache Maven/ {print $3}')
    MAVEN_MAJOR_VERSION=$(echo $MAVEN_VERSION | awk -F[.] '{print $1}')
    MAVEN_MINOR_VERSION=$(echo $MAVEN_VERSION | awk -F[.] '{print $2}')
    if [ "$MAVEN_MAJOR_VERSION" -ge 3 ] && [ "$MAVEN_MINOR_VERSION" -ge 6 ]; then
      echo "Maven version $MAVEN_VERSION is installed."
    else
      echo "Maven 3.6 or higher is required. Please install the required version of Maven."
      exit 1
    fi
  else
    echo "Maven is not installed. Please install Maven 3.6 or higher."
    exit 1
  fi
}

# Check for OS and run respective checks
case "$(uname -s)" in
  Linux*)
    echo "Running on Linux"
    check_java
    check_maven
    ;;
  Darwin*)
    echo "Running on macOS"
    check_java
    check_maven
    ;;
  CYGWIN*|MINGW*|MSYS*)
    echo "Running on Windows"
    check_java
    check_maven
    ;;
  *)
    echo "Unsupported OS"
    exit 1
    ;;
esac

# Continue with the build process
./mvnw clean package -P yugabyte -DskipTests
cd target
tar -xzf benchbase-yugabyte.tgz
cd benchbase-yugabyte
