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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * @author pavlo
 */
public abstract class ClassUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ClassUtil.class);

    private static final Map<Class<?>, List<Class<?>>> CACHE_getSuperClasses = new HashMap<>();
    private static final Map<Class<?>, Set<Class<?>>> CACHE_getInterfaceClasses = new HashMap<>();

    /**
     * Get the generic types for the given field
     *
     * @param field
     * @return
     */
    public static List<Class<?>> getGenericTypes(Field field) {
        ArrayList<Class<?>> generic_classes = new ArrayList<>();
        Type gtype = field.getGenericType();
        if (gtype instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType) gtype;
            getGenericTypesImpl(ptype, generic_classes);
        }
        return (generic_classes);
    }

    private static void getGenericTypesImpl(ParameterizedType ptype, List<Class<?>> classes) {
        // list the actual type arguments
        for (Type t : ptype.getActualTypeArguments()) {
            if (t instanceof Class) {
//                System.err.println("C: " + t);
                classes.add((Class<?>) t);
            } else if (t instanceof ParameterizedType) {
                ParameterizedType next = (ParameterizedType) t;
//                System.err.println("PT: " + next);
                classes.add((Class<?>) next.getRawType());
                getGenericTypesImpl(next, classes);
            }
        }
    }

    /**
     * Return an ordered list of all the sub-classes for a given class
     * Useful when dealing with generics
     *
     * @param element_class
     * @return
     */
    public static List<Class<?>> getSuperClasses(Class<?> element_class) {
        List<Class<?>> ret = ClassUtil.CACHE_getSuperClasses.get(element_class);
        if (ret == null) {
            ret = new ArrayList<>();
            while (element_class != null) {
                ret.add(element_class);
                element_class = element_class.getSuperclass();
            }
            ret = Collections.unmodifiableList(ret);
            ClassUtil.CACHE_getSuperClasses.put(element_class, ret);
        }
        return (ret);
    }

    /**
     * Get a set of all of the interfaces that the element_class implements
     *
     * @param element_class
     * @return
     */

    public static Collection<Class<?>> getInterfaces(Class<?> element_class) {
        Set<Class<?>> ret = ClassUtil.CACHE_getInterfaceClasses.get(element_class);
        if (ret == null) {
            ret = new HashSet<Class<?>>(ClassUtils.getAllInterfaces(element_class));
            if (element_class.isInterface()) {
                ret.add(element_class);
            }
            ret = Collections.unmodifiableSet(ret);
            ClassUtil.CACHE_getInterfaceClasses.put(element_class, ret);
        }
        return (ret);
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String class_name, Object[] params, Class<?>[] classes) {
        return ((T) ClassUtil.newInstance(ClassUtil.getClass(class_name), params, classes));
    }


    public static <T> T newInstance(Class<T> target_class, Object[] params, Class<?>[] classes) {
        Constructor<T> constructor = ClassUtil.getConstructor(target_class, classes);
        T ret = null;
        try {
            ret = constructor.newInstance(params);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create new instance of " + target_class.getSimpleName(), ex);
        }
        return (ret);
    }

    /**
     * @param <T>
     * @param target_class
     * @param params
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getConstructor(Class<T> target_class, Class<?>... params) {
        NoSuchMethodException error = null;
        try {
            return (target_class.getConstructor(params));
        } catch (NoSuchMethodException ex) {
            // The first time we get this it can be ignored
            // We'll try to be nice and find a match for them
            error = ex;
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("TARGET CLASS:  {}", target_class);
            LOG.debug("TARGET PARAMS: {}", Arrays.toString(params));
        }

        List<Class<?>>[] paramSuper = (List<Class<?>>[]) new List[params.length];
        for (int i = 0; i < params.length; i++) {
            paramSuper[i] = ClassUtil.getSuperClasses(params[i]);
            if (LOG.isDebugEnabled()) {
                LOG.debug("  SUPER[{}] => {}", params[i].getSimpleName(), paramSuper[i]);
            }
        }

        for (Constructor<?> c : target_class.getConstructors()) {
            Class<?>[] cTypes = c.getParameterTypes();
            if (LOG.isDebugEnabled()) {
                LOG.debug("CANDIDATE: {}", c);
                LOG.debug("CANDIDATE PARAMS: {}", Arrays.toString(cTypes));
            }
            if (params.length != cTypes.length) {
                continue;
            }

            for (int i = 0; i < params.length; i++) {
                List<Class<?>> cSuper = ClassUtil.getSuperClasses(cTypes[i]);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("  SUPER[{}] => {}", cTypes[i].getSimpleName(), cSuper);
                }
                if (!CollectionUtils.intersection(paramSuper[i], cSuper).isEmpty()) {
                    return ((Constructor<T>) c);
                }
            }
        }
        throw new RuntimeException("Failed to retrieve constructor for " + target_class.getSimpleName(), error);
    }

    /**
     * @param class_name
     * @return
     */
    public static Class<?> getClass(String class_name) {
        try {
            return ClassUtils.getClass(class_name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
