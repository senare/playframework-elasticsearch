/** 
 * Copyright 2011 The Apache Software Foundation
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
 * @author Felipe Oliveira (http://mashup.fm)
 * 
 */
package play.modules.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.db.Model;
import play.modules.elasticsearch.ElasticSearchIndexEvent.Type;
import play.modules.elasticsearch.adapter.ElasticSearchAdapter;
import play.modules.elasticsearch.annotations.ElasticSearchable;
import play.modules.elasticsearch.mapping.MapperFactory;
import play.modules.elasticsearch.mapping.MappingException;
import play.modules.elasticsearch.mapping.MappingUtil;
import play.modules.elasticsearch.mapping.ModelMapper;
import play.modules.elasticsearch.mapping.impl.DefaultMapperFactory;
import play.modules.elasticsearch.util.ReflectionUtil;
import play.mvc.Router;

// TODO: Auto-generated Javadoc
/**
 * The Class ElasticSearchPlugin.
 */
public class ElasticSearchPlugin extends PlayPlugin {

    /** The started. */
    private static boolean started = false;
    
    /** The mapper factory */
    private static MapperFactory mapperFactory = (MapperFactory) new DefaultMapperFactory();
    
    /** The mappers index. */
    private static Map<Class<?>, ModelMapper<?>> mappers = null;

    /** The started indices. */
    private static Set<Class<? extends Model>> indicesStarted = null;

    private static Set<Class<? extends Model>> riverStarted = null;

    private static Set<Class<? extends Model>> eligibleClasses = null;

    /** Index type -> Class lookup */
    private static Map<String, Class<?>> modelLookup = null;
    
    /** The client. */
    private static Client client = null;

    private static String clusterName = clusterName();

    public enum Mode { LOCAL, MEMORY, CLUSTER }

    private static Mode mode = mode();
    
    private static String clusterName() {
        try {
            return Play.configuration.getProperty("elasticsearch.cluster", "matchup");
        } catch (Exception e) {
            Logger.error("Error! Using default cluster name 'matchup' : %s", e);
            return "matchup";
        }
    }

    private static Mode mode() {
        try {
            return Mode.valueOf(Play.configuration.getProperty("elasticsearch.mode", "LOCAL").toUpperCase());
        } catch (Exception e) {
            Logger.error("Error! Using default mode 'LOCAL' : %s", e);
            return Mode.LOCAL;
        }
    }

    /**
     * Gets the delivery mode.
     * 
     * @return the delivery mode
     */
    public static ElasticSearchDeliveryMode getDeliveryMode() {
        String s = Play.configuration.getProperty("elasticsearch.delivery");
        if (s == null) {
            return ElasticSearchDeliveryMode.LOCAL;
        }
        return ElasticSearchDeliveryMode.valueOf(s.toUpperCase());
    }
    
    /**
     * Client.
     * 
     * @return the client
     */
    public static Client client() {
        return client;
    }
  
    /**
     * This method is called when the application starts - It will start ES
     * instance
     * 
     * @see play.PlayPlugin#onApplicationStart()
     */
    @Override
    public void onApplicationStart() {
        // (re-)set caches
        mappers = new HashMap<Class<?>, ModelMapper<?>>();
        indicesStarted = new HashSet<Class<? extends Model>>();
        riverStarted = new HashSet<Class<? extends Model>>();
        eligibleClasses = new HashSet<Class<? extends Model>>();
        ReflectionUtil.clearCache();

        // Make sure it doesn't get started more than once
        if ((client != null) || started) {
            Logger.debug("Elastic Search Started Already!");
            return;
        }

        NodeBuilder nodeBuilder = nodeBuilder();
        switch (mode) {
        case MEMORY:
            nodeBuilder.settings(ImmutableSettings.settingsBuilder()
                            .put("node.http.enabled", false)
                            .put("gateway.type", "none")
                            .put("index.gateway.type", "none")
                            .put("index.store.type", "memory")
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0).build())
                       .local(true)
                       .clusterName(clusterName);
            
            Logger.info("Starting Elastic Search for Play! (in memory)");
            break;
        case LOCAL:
            nodeBuilder.settings(ImmutableSettings.settingsBuilder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0).build())
               .local(false)
               .clusterName(clusterName);
            
            Logger.info("Starting Elastic Search for Play! (cluster name = %s , local = %s , shards = %s, replicas = %s)", clusterName, false, 1, 0);
            break;
        case CLUSTER:
            nodeBuilder.settings(ImmutableSettings.settingsBuilder()
                    .put("client.transport.sniff", true)
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 3).build())
               .local(true)
               .clusterName(clusterName);
            //TODO: Put sensible stuff here before this is used in production!
            Logger.info("Starting Elastic Search for Play! (cluster name = %s , local = %s , shards = %s, replicas = %s)", clusterName, true, 3, 3);
            break;
        default:
            Logger.info("Starting Elastic Search for Play! (default)");
        }

        client = nodeBuilder.node().client();

        // Check Client
        if (client == null) {
            throw new RuntimeException("Elastic Search Client cannot be null - please check the configuration provided and the health of your Elastic Search instances.");
        }

        for (Class clazz : Play.classloader.getAssignableClasses(Model.class)) {
            if (MappingUtil.isSearchable(clazz)) {
                // Dirty hack to get rid of test classes ... assumes that we
                // have all the model classes in the pkg models.*
                String pkg = clazz.getCanonicalName();
                if (pkg != null && pkg.startsWith("models") && !pkg.equals("models.elasticsearch.ElasticSearchSampleModel")) {
                    eligibleClasses.add(clazz);

                    if (true) {
                        startIndexIfNeeded(clazz);
                        startRiverIfNeeded(clazz);
                    }
                }
            }
        }
    }
    
    /**
     * This method is called when the application stops - It will stop ES
     * instance
     * 
     * @see play.PlayPlugin#onApplicationStop()
     */
    @Override    
    public void onApplicationStop() {
        Logger.info("Stoping Elastic Search for Play!");
        client.close();
    }
    
    private static void startIndexIfNeeded(Class<Model> clazz) {
        if (!indicesStarted.contains(clazz)) {
            ModelMapper<Model> mapper = getMapper(clazz);
            startIndex(clazz, mapper);
            indicesStarted.add(clazz);
        }
    }

    private static void startIndex(Class<Model> clazz, ModelMapper<Model> mapper) {
        Logger.info("Start Index for Class: %s", clazz);
        ElasticSearchAdapter.startIndex(client(), mapper);
    }

    private static void startRiverIfNeeded(Class<Model> clazz) {
        if (!riverStarted.contains(clazz)) {
            ModelMapper<Model> mapper = getMapper(clazz);
            startRiver(clazz, mapper);
            riverStarted.add(clazz);
        }
    }

    private static void startRiver(Class<Model> clazz, ModelMapper<Model> mapper) {
        if(mapper.getRiverSQL() != null){
            String driver = Play.configuration.getProperty("db.driver"); // "org.postgresql.Driver";
            String url = Play.configuration.getProperty("db.url"); // "jdbc:postgresql://127.0.0.1:5432/matchupdemo";
            String user = Play.configuration.getProperty("db.user"); // "matchupdemo";
            String password = Play.configuration.getProperty("db.pass"); // "qwerty";
            ElasticSearchAdapter.startRiver(client(), mapper, driver, url, user, password);
        }
    }

    static <T extends Model> void reIndex(Class<T> clazz) {
        if(clazz == null){
            throw new IllegalArgumentException("model is null");
        }
        
        if (!clazz.isAnnotationPresent(ElasticSearchable.class)) {
            throw new IllegalArgumentException("model is not searchable");
        }

        ModelMapper<Model> mapper = getMapper((Class<Model>) clazz);
        if (!indicesStarted.contains(clazz)) {
            ElasticSearchAdapter.deleteIndex(client, mapper);
            startIndex((Class<Model>) clazz, mapper);
        } else {
            startIndex((Class<Model>) clazz, mapper);
            indicesStarted.add(clazz);
        }

        if (!riverStarted.contains(clazz)) {
            ElasticSearchAdapter.deleteRiver(client, mapper);
            startRiver((Class<Model>) clazz, mapper);
        } else {
            startRiver((Class<Model>) clazz, mapper);
            riverStarted.add(clazz);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <M> ModelMapper<M> getMapper(final Class<M> clazz) {
        if (mappers.containsKey(clazz)) {
            return (ModelMapper<M>) mappers.get(clazz);
        }

        final ModelMapper<M> mapper = mapperFactory.getMapper(clazz);
        mappers.put(clazz, mapper);
        //modelLookup.put(mapper.getTypeName(), clazz);

        return mapper;
    }

    private static boolean isInterestingEvent(String event) {
        return event.endsWith(".objectPersisted") || event.endsWith(".objectUpdated") || event.endsWith(".objectDeleted");
    }

    /**
     * This is the method that will be sending data to ES instance
     * 
     * @see play.PlayPlugin#onEvent(java.lang.String, java.lang.Object)
     */
    @Override
    public void onEvent(String message, Object context) {
        // Log Debug
        Logger.info("Received %s Event, Object: %s", message, context);

        if (isInterestingEvent(message) == false) {
            return;
        }

        Logger.debug("Processing %s Event", message);

        // Check if object is searchable
        if (MappingUtil.isSearchable(context.getClass()) == false) {
            return;
        }

        // Sanity check, we only index models
        Validate.isTrue(context instanceof Model, "Only play.db.Model subclasses can be indexed");

        // Start index if needed
        @SuppressWarnings("unchecked")
        Class<Model> clazz = (Class<Model>) context.getClass();
        startIndexIfNeeded(clazz);

        // Define Event
        ElasticSearchIndexEvent event = null;
        if (message.endsWith(".objectPersisted") || message.endsWith(".objectUpdated")) {
            // Index Model
            event = new ElasticSearchIndexEvent((Model) context, ElasticSearchIndexEvent.Type.INDEX);

        } else if (message.endsWith(".objectDeleted")) {
            // Delete Model from Index
            event = new ElasticSearchIndexEvent((Model) context, ElasticSearchIndexEvent.Type.DELETE);
        }

        // Sync with Elastic Search
        Logger.info("Elastic Search Index Event: %s", event);
        if (event != null) {
            ElasticSearchDeliveryMode deliveryMode = getDeliveryMode();
            IndexEventHandler handler = deliveryMode.getHandler();
            handler.handle(event);
        }
    }

    <M extends Model> void index(M model) {
        @SuppressWarnings("unchecked")
        Class<Model> clazz = (Class<Model>) model.getClass();

        // Check if object is searchable
        if (MappingUtil.isSearchable(clazz) == false) {
            throw new IllegalArgumentException("model is not searchable");
        }

        startIndexIfNeeded(clazz);

        ElasticSearchIndexEvent event = new ElasticSearchIndexEvent(model, Type.INDEX);
        ElasticSearchDeliveryMode deliveryMode = getDeliveryMode();
        IndexEventHandler handler = deliveryMode.getHandler();
        handler.handle(event);
    }

    Set<Status> getEligibleClasses() {
        HashSet<Status> result = new HashSet<Status>();
        if (eligibleClasses != null) {
            for (Class clazz : eligibleClasses) {
                result.add(new Status(clazz, indicesStarted.contains(clazz), riverStarted.contains(clazz)));
            }
        }
        return result;
    }
    
    /**
     * Looks up the model class based on the index type name
     * 
     * @param indexType
     * @return Class of the Model
     */
    public static Class<?> lookupModel(final String indexType) {
        final Class<?> clazz = modelLookup.get(indexType);
        if (clazz != null) { // we have not cached this model yet
            return clazz;
        }
        final List<Class> searchableModels = Play.classloader.getAnnotatedClasses(ElasticSearchable.class);
        for (final Class searchableModel : searchableModels) {
            try {
                if (indexType.equals(getMapper(searchableModel).getTypeName())) {
                    return searchableModel;
                }
            } catch (final MappingException ex) {
                // mapper can not be retrieved
            }
        }
        throw new IllegalArgumentException("Type name '" + indexType + "' is not searchable!");
    }
}
