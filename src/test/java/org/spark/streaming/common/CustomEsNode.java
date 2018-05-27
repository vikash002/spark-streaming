package org.spark.streaming.common;

import org.elasticsearch.Version;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;


public class CustomEsNode extends Node {

    private Version version;
    private Collection<Class<? extends Plugin>> plugins;

    public CustomEsNode(Environment environment, Version version, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(environment, classpathPlugins);
        this.version = version;
        this.plugins = classpathPlugins;
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins;
    }

    public Version getVersion() {
        return version;
    }
}