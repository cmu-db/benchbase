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

package com.oltpbenchmark.util;


import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author pavlo
 */
public abstract class JSONUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JSONUtil.class.getName());

    private static final String JSON_CLASS_SUFFIX = "_class";
    private static final Map<Class<?>, Field[]> SERIALIZABLE_FIELDS = new HashMap<>();

    /**
     * @param clazz
     * @return
     */
    public static Field[] getSerializableFields(Class<?> clazz, String... fieldsToExclude) {
        Field[] ret = SERIALIZABLE_FIELDS.get(clazz);
        if (ret == null) {
            Collection<String> exclude = CollectionUtil.addAll(new HashSet<>(), fieldsToExclude);
            synchronized (SERIALIZABLE_FIELDS) {
                ret = SERIALIZABLE_FIELDS.get(clazz);
                if (ret == null) {
                    List<Field> fields = new ArrayList<>();
                    for (Field f : clazz.getFields()) {
                        int modifiers = f.getModifiers();
                        if (!Modifier.isTransient(modifiers) &&
                                Modifier.isPublic(modifiers) &&
                                !Modifier.isStatic(modifiers) &&
                                !exclude.contains(f.getName())) {
                            fields.add(f);
                        }
                    }
                    ret = fields.toArray(new Field[0]);
                    SERIALIZABLE_FIELDS.put(clazz, ret);
                }
            }
        }
        return (ret);
    }

    /**
     * JSON Pretty Print
     *
     * @param json
     * @return
     * @throws JSONException
     */
    public static String format(String json) {
        try {
            return (JSONUtil.format(new JSONObject(json)));
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String format(JSONObject o) {
        try {
            return o.toString(1);
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param object
     * @return
     */
    public static String toJSONString(Object object) {
        JSONStringer stringer = new JSONStringer();
        try {
            if (object instanceof JSONSerializable) {
                stringer.object();
                ((JSONSerializable) object).toJSON(stringer);
                stringer.endObject();
            } else if (object != null) {
                Class<?> clazz = object.getClass();
//                stringer.key(clazz.getSimpleName());
                JSONUtil.writeFieldValue(stringer, clazz, object);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return (stringer.toString());
    }

    public static <T extends JSONSerializable> T fromJSONString(T t, String json) {
        try {
            JSONObject json_object = new JSONObject(json);
            t.fromJSON(json_object);
        } catch (JSONException ex) {
            throw new RuntimeException("Failed to deserialize object " + t, ex);
        }
        return (t);
    }

    /**
     * Write the contents of a JSONSerializable object out to a file on the local disk
     *
     * @param <T>
     * @param object
     * @param output_path
     * @throws IOException
     */
    public static <T extends JSONSerializable> void save(T object, String output_path) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing out contents of {} to '{}'", object.getClass().getSimpleName(), output_path);
        }
        File f = new File(output_path);
        try {
            FileUtil.makeDirIfNotExists(f.getParent());
            String json = object.toJSONString();
            FileUtil.writeStringToFile(f, format(json));
        } catch (Exception ex) {
            LOG.error("Failed to serialize the {} file '{}'", object.getClass().getSimpleName(), f, ex);
            throw new IOException(ex);
        }
    }

    /**
     * Load in a JSONSerialable stored in a file
     *
     * @param object
     * @param input_path
     * @throws Exception
     */
    public static <T extends JSONSerializable> void load(T object, String input_path) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading in serialized {} from '{}'", object.getClass().getSimpleName(), input_path);
        }

        String contents;

        try (InputStream in = JSONUtil.class.getResourceAsStream(input_path)) {
            contents = IOUtils.toString(in, Charset.defaultCharset());
        }

        try {
            object.fromJSON(new JSONObject(contents));
        } catch (Exception ex) {
            LOG.error("Failed to deserialize the {} from file '{}'", object.getClass().getSimpleName(), input_path, ex);
            throw new IOException(ex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("The loading of the {} is complete", object.getClass().getSimpleName());
        }
    }

    /**
     * For a given list of Fields, write out the contents of the corresponding field to the JSONObject
     * The each of the JSONObject's elements will be the upper case version of the Field's name
     *
     * @param <T>
     * @param stringer
     * @param object
     * @param base_class
     * @param fields
     * @throws JSONException
     */
    public static <T> void fieldsToJSON(JSONStringer stringer, T object, Class<? extends T> base_class, Field[] fields) throws JSONException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Serializing out {} elements for {}", fields.length, base_class.getSimpleName());
        }
        for (Field f : fields) {
            String json_key = f.getName().toUpperCase();
            stringer.key(json_key);

            try {
                Class<?> f_class = f.getType();
                Object f_value = f.get(object);

                // Null
                if (f_value == null) {
                    writeFieldValue(stringer, f_class, f_value);
                    // Maps
                } else if (f_value instanceof Map) {
                    writeFieldValue(stringer, f_class, f_value);
                    // Everything else
                } else {
                    writeFieldValue(stringer, f_class, f_value);
                }
            } catch (Exception ex) {
                throw new JSONException(ex);
            }
        }
    }

    /**
     * @param stringer
     * @param field_class
     * @param field_value
     * @throws JSONException
     */
    public static void writeFieldValue(JSONStringer stringer, Class<?> field_class, Object field_value) throws JSONException {
        // Null
        if (field_value == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("writeNullFieldValue({}, {})", field_class, field_value);
            }
            stringer.value(null);

            // Collections
        } else if (ClassUtil.getInterfaces(field_class).contains(Collection.class)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("writeCollectionFieldValue({}, {})", field_class, field_value);
            }
            stringer.array();
            for (Object value : (Collection<?>) field_value) {
                if (value == null) {
                    stringer.value(null);
                } else {
                    writeFieldValue(stringer, value.getClass(), value);
                }
            }
            stringer.endArray();

            // Maps
        } else if (field_value instanceof Map) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("writeMapFieldValue({}, {})", field_class, field_value);
            }
            stringer.object();
            for (Entry<?, ?> e : ((Map<?, ?>) field_value).entrySet()) {
                // We can handle null keys
                String key_value = null;
                if (e.getKey() != null) {
                    // deserialize it on the other side
                    Class<?> key_class = e.getKey().getClass();
                    key_value = makePrimitiveValue(key_class, e.getKey()).toString();
                }
                stringer.key(key_value);

                // We can also handle null values. Where is your god now???
                if (e.getValue() == null) {
                    stringer.value(null);
                } else {
                    writeFieldValue(stringer, e.getValue().getClass(), e.getValue());
                }
            }
            stringer.endObject();

            // Primitive
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("writePrimitiveFieldValue({}, {})", field_class, field_value);
            }
            stringer.value(makePrimitiveValue(field_class, field_value));
        }
    }

    /**
     * Read data from the given JSONObject and populate the given Map
     *
     * @param json_object
     * @param map
     * @param inner_classes
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static void readMapField(final JSONObject json_object, final Map map, final Stack<Class> inner_classes) throws Exception {
        Class<?> key_class = inner_classes.pop();
        Class<?> val_class = inner_classes.pop();
        Collection<Class<?>> val_interfaces = ClassUtil.getInterfaces(val_class);


        for (String json_key : CollectionUtil.iterable(json_object.keys())) {
            final Stack<Class> next_inner_classes = new Stack<>();
            next_inner_classes.addAll(inner_classes);


            // KEY
            Object key = JSONUtil.getPrimitiveValue(json_key, key_class);

            // VALUE
            Object object = null;
            if (json_object.isNull(json_key)) {
                // Nothing...
            } else if (val_interfaces.contains(List.class)) {
                object = new ArrayList();
                readCollectionField(json_object.getJSONArray(json_key), (Collection) object, next_inner_classes);
            } else if (val_interfaces.contains(Set.class)) {
                object = new HashSet();
                readCollectionField(json_object.getJSONArray(json_key), (Collection) object, next_inner_classes);
            } else if (val_interfaces.contains(Map.class)) {
                object = new HashMap();
                readMapField(json_object.getJSONObject(json_key), (Map) object, next_inner_classes);
            } else {
                String json_string = json_object.getString(json_key);

                object = JSONUtil.getPrimitiveValue(json_string, val_class);

            }
            map.put(key, object);
        }
    }

    /**
     * Read data from the given JSONArray and populate the given Collection
     *
     * @param json_array
     * @param collection
     * @param inner_classes
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static void readCollectionField(final JSONArray json_array, final Collection collection, final Stack<Class> inner_classes) throws Exception {
        // We need to figure out what the inner type of the collection is
        // If it's a Collection or a Map, then we need to instantiate it before 
        // we can call readFieldValue() again for it.
        Class inner_class = inner_classes.pop();
        Collection<Class<?>> inner_interfaces = ClassUtil.getInterfaces(inner_class);

        for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
            final Stack<Class> next_inner_classes = new Stack<>();
            next_inner_classes.addAll(inner_classes);

            Object value = null;

            // Null
            if (json_array.isNull(i)) {
                value = null;
                // Lists
            } else if (inner_interfaces.contains(List.class)) {
                value = new ArrayList();
                readCollectionField(json_array.getJSONArray(i), (Collection) value, next_inner_classes);
                // Sets
            } else if (inner_interfaces.contains(Set.class)) {
                value = new HashSet();
                readCollectionField(json_array.getJSONArray(i), (Collection) value, next_inner_classes);
                // Maps
            } else if (inner_interfaces.contains(Map.class)) {
                value = new HashMap();
                readMapField(json_array.getJSONObject(i), (Map) value, next_inner_classes);
                // Values
            } else {
                String json_string = json_array.getString(i);
                value = JSONUtil.getPrimitiveValue(json_string, inner_class);
            }
            collection.add(value);
        }
    }

    /**
     * @param json_object
     * @param json_key
     * @param field_handle
     * @param object
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static void readFieldValue(final JSONObject json_object, final String json_key, Field field_handle, Object object) throws Exception {

        Class<?> field_class = field_handle.getType();
        Object field_object = field_handle.get(object);
        // String field_name = field_handle.getName();

        // Null
        if (json_object.isNull(json_key)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Field {} is null", json_key);
            }
            field_handle.set(object, null);

            // Collections
        } else if (ClassUtil.getInterfaces(field_class).contains(Collection.class)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Field {} is a collection", json_key);
            }

            Stack<Class> inner_classes = new Stack<>();
            inner_classes.addAll(ClassUtil.getGenericTypes(field_handle));
            Collections.reverse(inner_classes);

            JSONArray json_inner = json_object.getJSONArray(json_key);
            if (json_inner == null) {
                throw new JSONException("No array exists for '" + json_key + "'");
            }
            readCollectionField(json_inner, (Collection) field_object, inner_classes);

            // Maps
        } else if (field_object instanceof Map) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Field {} is a map", json_key);
            }

            Stack<Class> inner_classes = new Stack<>();
            inner_classes.addAll(ClassUtil.getGenericTypes(field_handle));
            Collections.reverse(inner_classes);

            JSONObject json_inner = json_object.getJSONObject(json_key);
            if (json_inner == null) {
                throw new JSONException("No object exists for '" + json_key + "'");
            }
            readMapField(json_inner, (Map) field_object, inner_classes);

            // Everything else...
        } else {
            Class explicit_field_class = JSONUtil.getClassForField(json_object, json_key);
            if (explicit_field_class != null) {
                field_class = explicit_field_class;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found explict field class {} for {}", field_class.getSimpleName(), json_key);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Field {} is primitive type {}", json_key, field_class.getSimpleName());
            }
            Object value = JSONUtil.getPrimitiveValue(json_object.getString(json_key), field_class);
            field_handle.set(object, value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Set field {} to '{}'", json_key, value);
            }
        }
    }

    /**
     * For the given list of Fields, load in the values from the JSON object into the current object
     * If ignore_missing is false, then JSONUtil will not throw an error if a field is missing
     *
     * @param <E>
     * @param <T>
     * @param json_object
     * @param object
     * @param base_class
     * @param ignore_missing
     * @param fields
     * @throws JSONException
     */
    public static <E extends Enum<?>, T> void fieldsFromJSON(JSONObject json_object, T object, Class<? extends T> base_class, boolean ignore_missing, Field... fields) throws JSONException {
        for (Field field_handle : fields) {
            String json_key = field_handle.getName().toUpperCase();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Retreiving value for field '{}'", json_key);
            }

            if (!json_object.has(json_key)) {
                String msg = "JSONObject for " + base_class.getSimpleName() + " does not have key '" + json_key + "': " + CollectionUtil.list(json_object.keys());
                if (ignore_missing) {
                    LOG.warn(msg);
                    continue;
                } else {
                    throw new JSONException(msg);
                }
            }

            try {
                readFieldValue(json_object, json_key, field_handle, object);
            } catch (Exception ex) {
                // System.err.println(field_class + ": " + ClassUtil.getSuperClasses(field_class));
                LOG.error("Unable to deserialize field '{}' from {}", json_key, base_class.getSimpleName(), ex);
                throw new JSONException(ex);
            }
        }
    }

    /**
     * Return the class of a field if it was stored in the JSONObject along with the value
     * If there is no class information, then this will return null
     *
     * @param json_object
     * @param json_key
     * @return
     * @throws JSONException
     */
    private static Class<?> getClassForField(JSONObject json_object, String json_key) throws JSONException {
        Class<?> field_class = null;
        // Check whether we also stored the class
        if (json_object.has(json_key + JSON_CLASS_SUFFIX)) {
            try {
                field_class = ClassUtil.getClass(json_object.getString(json_key + JSON_CLASS_SUFFIX));
            } catch (Exception ex) {
                LOG.error("Failed to include class for field '{}'", json_key, ex);
                throw new JSONException(ex);
            }
        }
        return (field_class);

    }

    /**
     * Return the proper serialization string for the given value
     *
     * @param field_class
     * @param field_value
     * @return
     */
    private static Object makePrimitiveValue(Class<?> field_class, Object field_value) {
        Object value = null;


        if (field_class.equals(Class.class)) {
            value = ((Class<?>) field_value).getName();
            // JSONSerializable
        } else if (ClassUtil.getInterfaces(field_class).contains(JSONSerializable.class)) {
            // Just return the value back. The JSON library will take care of it
//            System.err.println(field_class + ": " + field_value);
            value = field_value;
            // Everything else
        } else {
            value = field_value; // .toString();
        }
        return (value);
    }


    /**
     * For the given JSON string, figure out what kind of object it is and return it
     *
     * @param json_value
     * @param field_class
     * @return
     */
    public static Object getPrimitiveValue(String json_value, Class<?> field_class) {
        Object value = null;


        if (field_class.equals(Class.class)) {
            value = ClassUtil.getClass(json_value);
            if (value == null) {
                throw new JSONException("Failed to get class from '" + json_value + "'");
            }
            // Enum
        } else if (field_class.isEnum()) {
            for (Object o : field_class.getEnumConstants()) {
                Enum<?> e = (Enum<?>) o;
                if (json_value.equals(e.name())) {
                    return (e);
                }
            }
            throw new JSONException("Invalid enum value '" + json_value + "': " + Arrays.toString(field_class.getEnumConstants()));
            // JSONSerializable
        } else if (ClassUtil.getInterfaces(field_class).contains(JSONSerializable.class)) {
            value = ClassUtil.newInstance(field_class, null, null);
            ((JSONSerializable) value).fromJSON(new JSONObject(json_value));
            // Boolean
        } else if (field_class.equals(Boolean.class) || field_class.equals(boolean.class)) {
            // We have to use field_class.equals() because the value may be null
            value = Boolean.parseBoolean(json_value);
            // Short
        } else if (field_class.equals(Short.class) || field_class.equals(short.class)) {
            value = Short.parseShort(json_value);
            // Integer
        } else if (field_class.equals(Integer.class) || field_class.equals(int.class)) {
            value = Integer.parseInt(json_value);
            // Long
        } else if (field_class.equals(Long.class) || field_class.equals(long.class)) {
            value = Long.parseLong(json_value);
            // Float
        } else if (field_class.equals(Float.class) || field_class.equals(float.class)) {
            value = Float.parseFloat(json_value);
            // Double
        } else if (field_class.equals(Double.class) || field_class.equals(double.class)) {
            value = Double.parseDouble(json_value);
            // String
        } else if (field_class.equals(String.class)) {
            value = json_value;
        }
        return (value);
    }

}
