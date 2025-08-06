import argparse
import json
import os
import sys
import time
import uuid
import subprocess

def run_command(cmd):
    """Run a command and return its output."""
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(cmd)}")
        print(f"Error output: {e.stderr}")
        sys.exit(1)

def upload_benchbase_directory_to_volume(benchbase_dir_path, volume_path="/Volumes/loge-manav/loge-schema/loge-vol"):
    """Upload entire BenchBase directory to Databricks volume and return the volume path."""
    import os
    import time
    
    # Extract directory name from path
    dir_name = os.path.basename(benchbase_dir_path.rstrip('/'))
    volume_benchbase_path = f"{volume_path}/{dir_name}_{int(time.time())}"
    
    print(f"Uploading {benchbase_dir_path} to volume path: {volume_benchbase_path}")
    
    # Upload entire directory to volume
    run_command(["databricks", "fs", "cp", "-r", benchbase_dir_path, f"{volume_benchbase_path}", "--overwrite"])
    
    return volume_benchbase_path

def upload_jar_to_dbfs(jar_path):
    """Upload JAR file to DBFS and return the DBFS path."""
    jar_name = os.path.basename(jar_path)
    dbfs_path = f"/tmp/{jar_name}"
    
    print(f"Uploading {jar_path} to DBFS path: {dbfs_path}")
    run_command(["databricks", "fs", "cp", jar_path, f"dbfs:{dbfs_path}", "--overwrite"])
    
    return dbfs_path

def create_java23_init_script():
    """Create an init script for installing Java 23."""
    init_script_content = """#!/bin/bash
# Install Java 23 on Databricks cluster

# Download and install Java 23
cd /tmp
wget -q https://download.oracle.com/java/23/latest/jdk-23_linux-x64_bin.tar.gz
tar -xzf jdk-23_linux-x64_bin.tar.gz
sudo mv jdk-23.* /usr/lib/jvm/java-23

# Set Java 23 as default
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-23/bin/java 1
sudo update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-23/bin/javac 1
sudo update-alternatives --set java /usr/lib/jvm/java-23/bin/java
sudo update-alternatives --set javac /usr/lib/jvm/java-23/bin/javac

# Set environment variables
echo 'export JAVA_HOME=/usr/lib/jvm/java-23' >> /etc/environment
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/environment

# Verify installation
/usr/lib/jvm/java-23/bin/java -version
"""
    return init_script_content

def upload_init_script_for_java23(script_content):
    """Upload the Java 23 init script to DBFS."""
    script_path = f"/tmp/java23_init_script_{int(time.time())}.sh"
    local_script_path = f"/tmp/local_java23_init_script_{int(time.time())}.sh"
    
    # Write script to local file
    with open(local_script_path, 'w') as f:
        f.write(script_content)
    
    # Upload to DBFS
    print(f"Uploading Java 23 init script to DBFS path: {script_path}")
    run_command(["databricks", "fs", "cp", local_script_path, f"dbfs:{script_path}", "--overwrite"])
    
    # Clean up local file
    os.remove(local_script_path)
    
    return script_path

def create_spark_job(jar_path, main_class, job_name, parameters, 
                     cluster_id=None, new_cluster_conf=None, 
                     java_version=None, use_init_script=False, use_volume=False, volume_path=None):
    """Create a Spark job using the JAR file with specified Java version."""
    
    if use_volume and volume_path:
        # When using volume, the JAR is part of a complete BenchBase installation
        jar_path_for_job = jar_path  # jar_path already includes the volume path
        # Add working directory parameter to run BenchBase from the correct location
        working_dir_params = ["--working-dir", volume_path]
        parameters = working_dir_params + parameters
    else:
        # Traditional DBFS JAR upload - jar_path is already the DBFS path
        jar_path_for_job = f"dbfs:{jar_path}"
    
    # Create the task configuration
    task_config = {
        "task_key": "main_task",
        "libraries": [{"jar": jar_path_for_job}],
        "spark_jar_task": {
            "main_class_name": main_class,
            "parameters": parameters
        }
    }
    
    # Add cluster configuration to the task
    if cluster_id:
        task_config["existing_cluster_id"] = cluster_id
        # Note: Java version settings are ignored when using existing cluster
        # The cluster must already be configured with the correct Java version
        if java_version:
            print(f"Warning: Java version {java_version} setting ignored when using existing cluster {cluster_id}")
            print(f"Please ensure the existing cluster is configured with Java {java_version}")
    else:
        # Default cluster configuration if none provided
        default_cluster = {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "r3.xlarge",
            "num_workers": 2,
            "spark_conf": {
                "spark.speculation": "true"
            }
        }
        
        # Use provided cluster configuration or default
        cluster_config = new_cluster_conf if new_cluster_conf else default_cluster
        
        # Configure Java version if specified
        if java_version:
            # Add Java version configuration
            if "spark_conf" not in cluster_config:
                cluster_config["spark_conf"] = {}
                
            cluster_config["spark_conf"]["spark.executorEnv.JAVA_HOME"] = f"/usr/lib/jvm/java-{java_version}"
            cluster_config["spark_conf"]["spark.yarn.appMasterEnv.JAVA_HOME"] = f"/usr/lib/jvm/java-{java_version}"
            cluster_config["spark_conf"]["spark.databricks.cluster.customTags.JavaVersion"] = str(java_version)
        
        # Add init script for Java 23 if requested
        if use_init_script and java_version == 23:
            # Create and upload init script
            init_script = create_java23_init_script()
            init_script_path = upload_init_script_for_java23(init_script)
            
            # Add init script to cluster config
            cluster_config["init_scripts"] = [
                {"dbfs": {"destination": init_script_path}}
            ]
        
        task_config["new_cluster"] = cluster_config
    
    # Prepare the job configuration with tasks array
    job_config = {
        "name": job_name,
        "tasks": [task_config]
    }
    
    # Write the job configuration to a temporary file
    config_file = f"/tmp/job_config_{uuid.uuid4()}.json"
    with open(config_file, 'w') as f:
        json.dump(job_config, f, indent=2)
    
    # Create the job
    result = run_command(["databricks", "jobs", "create", "--json", f"@{config_file}"])
    
    # Clean up the temporary file
    os.remove(config_file)
    
    # Parse and return the job ID
    job_data = json.loads(result)
    job_id = job_data.get("job_id")
    
    print(f"Created Spark job with ID: {job_id}")
    print(f"Job configuration: {json.dumps(job_config, indent=2)}")
    
    return job_id

def run_job(job_id):
    """Run the job and return the run ID."""
    result = run_command(["databricks", "jobs", "run-now", str(job_id)])
    run_data = json.loads(result)
    run_id = run_data.get("run_id")
    
    print(f"Started job run with ID: {run_id}")
    return run_id

def main():
    parser = argparse.ArgumentParser(
        description="Create a Databricks Spark job from a JAR file",
        epilog="""
Examples:
  # Create new cluster and run job with volume (recommended):
  python3 spark_job.py /path/to/benchbase-postgres --main-class com.oltpbenchmark.DBWorkload --use-volume --java-version 17 --run-now -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true

  # Use existing cluster with volume:
  python3 spark_job.py /path/to/benchbase-postgres --main-class com.oltpbenchmark.DBWorkload --use-volume --cluster-id "0123-456789-abcd" -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true

  # Legacy JAR-only mode:
  python3 spark_job.py benchbase.jar --main-class com.oltpbenchmark.DBWorkload --workers 4 --worker-type r3.2xlarge -b tpcc
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("jar_or_dir_path", help="Path to the JAR file or BenchBase directory")
    parser.add_argument("--main-class", required=True, help="Main class to execute")
    parser.add_argument("--job-name", default=f"Spark Job {int(time.time())}", help="Name for the job")
    parser.add_argument("--cluster-id", help="Existing cluster ID to use")
    parser.add_argument("--workers", type=int, default=2, help="Number of workers if creating a new cluster")
    parser.add_argument("--worker-type", default="r3.xlarge", help="Worker node type if creating a new cluster")
    parser.add_argument("--spark-version", default="11.3.x-scala2.12", help="Spark version to use")
    parser.add_argument("--java-version", type=int, choices=[8, 11, 17, 23], default=None, 
                        help="Java version to use (8, 11, 17, or 23)")
    parser.add_argument("--use-init-script", action="store_true", 
                        help="Use init script to install Java 23 (required for Java 23)")
    parser.add_argument("--use-volume", action="store_true", 
                        help="Upload entire BenchBase directory to volume instead of just JAR to DBFS")
    parser.add_argument("--volume-path", default="/Volumes/loge-manav/loge-schema/loge-vol",
                        help="Volume path for uploading BenchBase directory")
    parser.add_argument("--run-now", action="store_true", help="Run the job immediately after creation")
    
    # Parse known args to separate script args from main class parameters
    args, unknown_args = parser.parse_known_args()
    
    # All unknown arguments will be passed as parameters to the main class
    main_class_params = unknown_args
    
    # Check if the path exists
    if not os.path.exists(args.jar_or_dir_path):
        print(f"Error: Path not found: {args.jar_or_dir_path}")
        sys.exit(1)
    
    # Enforce init script for Java 23
    if args.java_version == 23 and not args.use_init_script:
        print("Java 23 requires using an init script. Setting --use-init-script=True")
        args.use_init_script = True
    
    # Upload the JAR or directory based on mode
    if args.use_volume:
        if os.path.isdir(args.jar_or_dir_path):
            # Upload entire directory to volume
            volume_benchbase_path = upload_benchbase_directory_to_volume(args.jar_or_dir_path, args.volume_path)
            jar_path_for_job = f"{volume_benchbase_path}/benchbase.jar"
        else:
            print("Error: --use-volume requires a directory path, not a JAR file")
            sys.exit(1)
    else:
        if os.path.isfile(args.jar_or_dir_path):
            # Traditional JAR upload to DBFS
            jar_path_for_job = upload_jar_to_dbfs(args.jar_or_dir_path)
            volume_benchbase_path = None
        else:
            print("Error: JAR mode requires a JAR file path, not a directory")
            sys.exit(1)
    
    # Create cluster configuration if not using an existing cluster
    new_cluster_conf = None
    if not args.cluster_id:
        new_cluster_conf = {
            "spark_version": args.spark_version,
            "node_type_id": args.worker_type,
            "num_workers": args.workers,
            "spark_conf": {
                "spark.speculation": "true"
            }
        }
    
    # Create the Spark job
    job_id = create_spark_job(
        jar_path_for_job, 
        args.main_class, 
        args.job_name, 
        main_class_params,  # Pass the unknown args as parameters
        args.cluster_id, 
        new_cluster_conf,
        args.java_version,
        args.use_init_script,
        args.use_volume,
        volume_benchbase_path
    )
    
    # Run the job if requested
    if args.run_now:
        run_id = run_job(job_id)
        print(f"Job is now running. Check the Databricks UI for job run ID: {run_id}")
    else:
        print(f"Job created but not started. Start it from the Databricks UI or with:")
        print(f"  databricks jobs run-now {job_id}")
    
    print(f"Main class parameters being passed: {main_class_params}")

if __name__ == "__main__":
    main()
