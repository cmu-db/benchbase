package com.oltpbenchmark.benchmarks.dataloader;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.FeatureBenchLoader;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class DataGenerator extends BenchmarkModule {
    /**
     * Constructor!
     *
     * @param workConf
     */
    public DataGenerator(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        return null;
    }

    @Override
    protected Loader<DataGenerator> makeLoaderImpl() {
        // load properties file

        return new DataGeneratorLoader(this, getProperties("datatype-mapping.properties"),
            getProperties("pk-mapping.properties"),
            getProperties("array-mapping.properties"), getFkProperties());
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return null;
    }

    public Map<String, PropertyMapping> getProperties(String propertiesType) {
        Properties properties = new Properties();
        Map<String, PropertyMapping> propertyMap = new LinkedHashMap<>();
        final String path = "/benchmarks/" + getBenchmarkName() + "/" + propertiesType;

        try (InputStream input = this.getClass().getResourceAsStream(path)) {
            properties.load(input);
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                String[] parts = value.split(":");
                String className = parts[0];
                int numParams = Integer.parseInt(parts[1]);
                List<Object> params = new ArrayList<>();
                if (numParams > 0) {
                    params.addAll(Arrays.asList(parts[2].split(",")));
                }

                PropertyMapping property = new PropertyMapping(className, numParams, params);
                propertyMap.put(key, property);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return propertyMap;
    }

    public Map<String, FkPropertyMapping> getFkProperties() {
        Properties properties = new Properties();
        Map<String, FkPropertyMapping> propertyMap = new LinkedHashMap<>();
        final String path = "/benchmarks/" + getBenchmarkName() + "/fk-mapping.properties";

        try (InputStream input = this.getClass().getResourceAsStream(path)) {
            properties.load(input);
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                String[] parts = value.split(":");
                String className = parts[0];
                String dataType = parts[1];

                FkPropertyMapping property = new FkPropertyMapping(className, dataType);
                propertyMap.put(key, property);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return propertyMap;
    }
}
