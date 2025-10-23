package com.netcracker.core.declarative.client.reconciler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.DefaultBean;
import io.quarkus.restclient.runtime.QuarkusRestClientBuilder;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import com.netcracker.cloud.consul.provider.common.TokenStorage;
import com.netcracker.core.declarative.client.rest.CompositeClient;
import com.netcracker.core.declarative.client.rest.DeclarativeClient;
import com.netcracker.core.declarative.client.rest.deprecated.MeshClientV3;
import com.netcracker.core.declarative.service.*;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.netcracker.core.declarative.client.reconciler.CompositeReconciler.DBAAS_NAME;
import static com.netcracker.core.declarative.client.reconciler.CompositeReconciler.MAAS_NAME;

@Slf4j
public class Configuration {
    @Produces
    @Named("maasDeclarativeClient")
    @ApplicationScoped
    public DeclarativeClient maasDeclarativeClient(@ConfigProperty(name = "quarkus.rest-client.maas-client.url") String maasUrl, RestClientCustomizer restClientCustomizer) {
        return createXaasDeclarativeClient(maasUrl, restClientCustomizer);
    }

    @Produces
    @Named("meshDeclarativeClient")
    @ApplicationScoped
    public MeshClientV3 meshDeclarativeClient(@ConfigProperty(name = "quarkus.rest-client.mesh-client-v3.url") String meshUrl, RestClientCustomizer restClientCustomizer) {
        return restClientCustomizer.customize(new QuarkusRestClientBuilder().baseUri(URI.create(meshUrl)))
                .build(MeshClientV3.class);
    }

    @Produces
    @Named("idpExtensionsDeclarativeClient")
    @ApplicationScoped
    public DeclarativeClient idpExtensionsDeclarativeClient(@ConfigProperty(name = "quarkus.rest-client.idp-extensions-client.url") String idpExtensionsUrl, RestClientCustomizer restClientCustomizer) {
        return createXaasDeclarativeClient(idpExtensionsUrl, restClientCustomizer);
    }

    @Produces
    @Named("keyManagerDeclarativeClient")
    @ApplicationScoped
    public DeclarativeClient keyManagerDeclarativeClient(@ConfigProperty(name = "quarkus.rest-client.key-manager-client.url") String keyManagerUrl, RestClientCustomizer restClientCustomizer) {
        return createXaasDeclarativeClient(keyManagerUrl, restClientCustomizer);
    }

    @Produces
    @Named("dbaasDeclarativeClient")
    @ApplicationScoped
    public DeclarativeClient dbaasDeclarativeClient(@ConfigProperty(name = "quarkus.rest-client.dbaas-client.url") String dbaasUrl, RestClientCustomizer restClientCustomizer) {
        return createXaasDeclarativeClient(dbaasUrl, restClientCustomizer);
    }

    @Produces
    @ApplicationScoped
    public List<CompositeStructureUpdateNotifier> compositeStructureUpdateNotifier(
            @ConfigProperty(name = "quarkus.rest-client.maas-client.url") String maasUrl,
            @ConfigProperty(name = "quarkus.rest-client.dbaas-client.url") String dbaasUrl,
            @ConfigProperty(name = "cloud.composite.structure.xaas.receivers") List<String> receiversConfig,
            @ConfigProperty(name = "cloud.composite.structure.xaas.read-timeout") Long readTimeout,
            @ConfigProperty(name = "cloud.composite.structure.xaas.connect-timeout") Long connectTimeout,
            RestClientCustomizer restClientCustomizer
    ) {
        List<String> receiversConfigLowercase = receiversConfig.stream().map(String::toLowerCase).toList();
        return Map.of(
                        MAAS_NAME, maasUrl,
                        DBAAS_NAME, dbaasUrl
                )
                .entrySet()
                .stream()
                // take only XaaSes enlisted in receivers config compare ignoring case
                .filter(xaas -> receiversConfigLowercase.contains(xaas.getKey().toLowerCase()))
                .map(xaas -> new CompositeStructureUpdateNotifier(xaas.getKey(), // construct notifier instances for matched XaaSes
                                restClientCustomizer.customize(new QuarkusRestClientBuilder()
                                                .baseUri(URI.create(xaas.getValue()))
                                                .readTimeout(readTimeout, TimeUnit.MILLISECONDS)
                                                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS))
                                        .build(CompositeClient.class)
                        )
                )
                .toList();
    }

    @Produces
    @ApplicationScoped
    public CompositeConsulUpdater compositeConsulUpdater(
            @ConfigProperty(name = "cloud.microservice.namespace") String namespace,
            @ConfigProperty(name = "quarkus.consul-source-config.agent.url") URL consulUrl,
            @ConfigProperty(name = "quarkus.consul-source-config.agent.enabled") boolean consulEnabled,
            @ConfigProperty(name = "cloud.composite.structure.consul.update-timeout") Long timeout,
            ConsulClientFactory consulClientFactory,
            Instance<TokenStorage> consulTokenStorage) { // TokenStorage in Singleton scope. Lazy inject.
        if (!consulEnabled) {
            return new NoopCompositeConsulUpdaterImpl();
        }
        return new CompositeConsulUpdaterImpl(namespace, consulClientFactory, consulTokenStorage.get());
    }

    @Produces
    @ApplicationScoped
    public CompositeWatcherManager compositeWatcherManager(
            @ConfigProperty(name = "cloud.microservice.namespace") String namespace,
            @ConfigProperty(name = "quarkus.consul-source-config.agent.enabled") boolean consulEnabled,
            ConsulClientFactory consulClientFactory,
            Instance<TokenStorage> consulTokenStorage) {
        if (!consulEnabled) {
            return null; //todo vlla
        }
        return new CompositeWatcherManager(namespace, consulClientFactory, consulTokenStorage.get());
    }

    @Produces
    @ApplicationScoped
    public ConsulClientFactory consulClientFactory(Vertx vertx,
                                                   @ConfigProperty(name = "quarkus.consul-source-config.agent.url") URL consulUrl,
                                                   @ConfigProperty(name = "cloud.composite.structure.consul.update-timeout") Long timeout) {
        return token ->
                ConsulClient.create(vertx.getDelegate(), new ConsulClientOptions()
                        .setHost(consulUrl.getHost())
                        .setPort(consulUrl.getPort())
                        .setTimeout(timeout)
//                        .setAclToken(token) //tod vlla
                );
    }

    @Produces
    @ApplicationScoped
    public TenantService tenantService(ConsulClientFactory consulClientFactory, Instance<TokenStorage> consulTokenStorage) {
        return new TenantService(consulClientFactory, consulTokenStorage.get());
    }

    @Produces
    @ApplicationScoped
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Produces
    @DefaultBean
    @ApplicationScoped
    public RestClientCustomizer restClientCustomizer() {
        return builder -> builder;
    }

    public interface RestClientCustomizer {
        RestClientBuilder customize(RestClientBuilder builder);
    }

    private DeclarativeClient createXaasDeclarativeClient(String xaasUrl, RestClientCustomizer restClientCustomizer) {
        return restClientCustomizer.customize(new QuarkusRestClientBuilder()
                        .baseUri(URI.create(xaasUrl)))
                .build(DeclarativeClient.class);
    }
}
