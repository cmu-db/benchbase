/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.oltpbenchmark.benchmarks.templated;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.templates.TemplateType;
import com.oltpbenchmark.api.templates.TemplatesType;
import com.oltpbenchmark.api.templates.ValueType;
import com.oltpbenchmark.api.templates.ValuesType;
import com.oltpbenchmark.benchmarks.templated.procedures.GenericQuery;
import com.oltpbenchmark.benchmarks.templated.procedures.GenericQuery.QueryTemplateInfo;
import com.oltpbenchmark.benchmarks.templated.util.GenericQueryOperation;
import com.oltpbenchmark.benchmarks.templated.util.TemplatedValue;
import com.oltpbenchmark.benchmarks.templated.util.TraceTransactionGenerator;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.Unmarshaller;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.commons.text.StringEscapeUtils;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to execute templated benchmarks, i.e., benchmarks that have parameters that
 * the user wants to set dynamically. More information about the structure of the expected template
 * can be found in the local readme file.
 */
public final class TemplatedBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(TemplatedBenchmark.class);

  public TemplatedBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
    this.setClassLoader();
  }

  @Override
  protected void setClassLoader() {
    super.setClassLoader();

    if (workConf != null && workConf.getXmlConfig().containsKey("query_templates_file")) {
      this.classLoader =
          this.loadQueryTemplates(workConf.getXmlConfig().getString("query_templates_file"));
    } else {
      LOG.error("No query_templates_file specified in xml config.");
    }
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return (GenericQuery.class.getPackage());
  }

  private CustomClassLoader getCustomClassLoader() {
    return (CustomClassLoader) this.classLoader;
  }

  public List<Class<? extends Procedure>> getProcedureClasses() {
    return this.getCustomClassLoader().getProcedureClasses();
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

    try {
      final Map<Class<? extends Procedure>, TraceTransactionGenerator> generators = new HashMap<>();
      // Create potential parameter bindings for each template. Add those
      // to a trace transaction generator that will determine how the
      // parameters are used.
      for (Entry<TransactionType, Procedure> kv : getProcedures().entrySet()) {
        // Sanity check that the procedure has the right type.
        if (!(kv.getValue() instanceof GenericQuery)) {
          LOG.error(
              String.format(
                  "Procedure %s does not have the correct class type (GenericQuery).",
                  kv.getValue().toString()));
          continue;
        }
        GenericQuery proc = (GenericQuery) kv.getValue();
        QueryTemplateInfo info = proc.getQueryTemplateInfo();

        // Parse parameter values and add each combination to a generator.
        // FIXME: This method does not currently support NULLable
        // parameters since they will be parsed as an empty string.
        // See Also: comments in GenericQuery.getStatement()
        // Additionally, it's somewhat unnecessarily expensive, since
        // we convert from XML represented values back to CSV separated
        // list of params.
        List<GenericQueryOperation> list = new ArrayList<>();
        String[] paramsTypes = info.getParamsTypes();

        TemplatedValue[] params = info.getParamsValues();
        int paramsLen = params.length;
        int typesLen = paramsTypes.length;

        assert (paramsLen % typesLen) == 0;

        if (paramsLen == typesLen) {
          list.add(new GenericQueryOperation(params));
        } else {
          int numSplits = paramsLen / typesLen;
          for (int j = 0; j < numSplits; j += 1) {
            TemplatedValue[] subset =
                Arrays.copyOfRange(params, j * typesLen, j * typesLen + typesLen);
            assert subset.length == typesLen;
            list.add(new GenericQueryOperation(subset));
          }
        }

        generators.put(proc.getClass(), new TraceTransactionGenerator(list));
      }

      // Create workers.
      int numTerminals = workConf.getTerminals();
      LOG.info(String.format("Creating %d workers for templated benchmark", numTerminals));
      for (int i = 0; i < numTerminals; i++) {
        workers.add(new TemplatedWorker(this, i, generators));
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to create workers", e);
    }

    return workers;
  }

  @Override
  protected Loader<TemplatedBenchmark> makeLoaderImpl() {
    throw new UnsupportedOperationException(
        "Templated benchmarks do not currently support loading directly.");
  }

  private CustomClassLoader loadQueryTemplates(String file) {
    // Instantiate Java compiler.
    CustomClassLoader ccloader = new CustomClassLoader(this.classLoader);
    try {
      // Parse template file.
      final ICompilerFactory compilerFactory =
          CompilerFactoryFactory.getDefaultCompilerFactory(
              TemplatedBenchmark.class.getClassLoader());

      JAXBContext jc = JAXBContext.newInstance("com.oltpbenchmark.api.templates");
      SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema schema =
          sf.newSchema(new StreamSource(this.getClass().getResourceAsStream("/templates.xsd")));
      Unmarshaller unmarshaller = jc.createUnmarshaller();
      unmarshaller.setSchema(schema);

      StreamSource streamSource = new StreamSource(new FileInputStream(file));
      JAXBElement<TemplatesType> result = unmarshaller.unmarshal(streamSource, TemplatesType.class);
      TemplatesType templates = result.getValue();

      for (TemplateType template : templates.getTemplateList()) {
        ImmutableParsedQueryTemplate.Builder b = ImmutableParsedQueryTemplate.builder();
        List<String> templateTypes = template.getTypes().getTypeList();

        b.name(template.getName());
        b.query(template.getQuery());
        b.paramsTypes(templateTypes);

        for (ValuesType paramValue : template.getValuesList()) {
          int typeIndex = 0;
          for (ValueType value : paramValue.getValueList()) {
            /* Lightweight constructor used if no distribution is present */
            if (value.getDist() == null
                || value.getDist().length() < 1
                || value.getDist().equals("null")) {
              b.addParamsValues(new TemplatedValue(value.getValue()));
            } else {
              b.addParamsValues(
                  new TemplatedValue(
                      value.getDist(),
                      value.getMin(),
                      value.getMax(),
                      value.getSeed(),
                      templateTypes.get(typeIndex)));
            }
            typeIndex++;
          }
        }

        ParsedQueryTemplate qt = b.build();
        // Create and compile class.
        final String s =
                """
                        package %s ;
                        import %s ;
                        public final class %s extends %s {
                            @Override
                            public %s getQueryTemplateInfo() {
                                return ImmutableQueryTemplateInfo.builder()
                                        .query(new %s(\"%s\"))
                                        .paramsTypes(new String[] {%s})
                                        .paramsValues(new TemplatedValue[] {%s})
                                        .build();
                            }
                        }
                        """
                .formatted(
                    GenericQuery.class.getPackageName(),
                    TemplatedValue.class.getCanonicalName(),
                    qt.getName(),
                    GenericQuery.class.getCanonicalName(),
                    QueryTemplateInfo.class.getCanonicalName(),
                    SQLStmt.class.getCanonicalName(),
                    StringEscapeUtils.escapeJava(qt.getQuery()),
                    getParamsString(qt.getParamsTypes()),
                    buildTemplatedValueString(qt.getParamsValues()));
        LOG.debug("Class definition for query template {}:\n {}", qt.getName(), s);
        final String qualifiedClassName = GenericQuery.class.getPackageName() + "." + qt.getName();
        final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
        compiler.setTargetVersion(17);
        compiler.setParentClassLoader(this.classLoader);
        compiler.cook(s);
        ccloader.putClass(
            qualifiedClassName, compiler.getClassLoader().loadClass(qualifiedClassName));
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to load query templates", e);
    }
    return ccloader;
  }

  private String getParamsString(List<String> params) {
    String result = "";
    for (String param : params) {
      result += "\"" + StringEscapeUtils.escapeJava(param) + "\",";
    }
    return result.isEmpty() ? "" : result.substring(0, result.length() - 1);
  }

  private String buildTemplatedValueString(List<TemplatedValue> params) {
    String result = "";
    for (TemplatedValue param : params) {
      if (param.getDistribution() != null) {
        result +=
            "new TemplatedValue("
                + "\""
                + param.getDistribution()
                + "\""
                + ","
                + "\""
                + param.getMin()
                + "\""
                + ","
                + "\""
                + param.getMax()
                + "\""
                + ","
                + "\""
                + param.getSeed()
                + "\""
                + ","
                + "\""
                + param.getValueType()
                + "\""
                + "),";
      } else {
        result +=
            "new TemplatedValue("
                + "\""
                + StringEscapeUtils.escapeJava(param.getValue())
                + "\""
                + "),";
      }
    }
    return result.isEmpty() ? "" : result.substring(0, result.length() - 1);
  }

  private static class CustomClassLoader extends ClassLoader {

    private final Map<String, Class<?>> classes = new HashMap<>();

    private CustomClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      Class<?> clazz = classes.get(name);
      return clazz != null ? clazz : super.findClass(name);
    }

    public void putClass(String name, Class<?> clazz) {
      classes.put(name, clazz);
    }

    @SuppressWarnings("unchecked")
    public List<Class<? extends Procedure>> getProcedureClasses() {
      List<Class<? extends Procedure>> result = new ArrayList<>();
      for (Class<?> clz : classes.values()) {
        if (Procedure.class.isAssignableFrom(clz)) {
          result.add((Class<? extends Procedure>) clz);
        }
      }
      return result;
    }
  }

  @Value.Immutable
  public interface ParsedQueryTemplate {

    /** Template name. */
    String getName();

    /** Query string for this template. */
    String getQuery();

    /** Potential query parameter types. */
    @Value.Default
    default List<String> getParamsTypes() {
      return List.of();
    }

    /** Potential query parameter values. */
    @Value.Default
    default List<TemplatedValue> getParamsValues() {
      return List.of();
    }
  }
}
