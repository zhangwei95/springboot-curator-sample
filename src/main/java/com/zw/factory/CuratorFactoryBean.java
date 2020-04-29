package com.zw.factory;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;

public class CuratorFactoryBean implements FactoryBean<CuratorFramework>, InitializingBean, DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorFactoryBean.class);

    private String connectionString;
    private int sessionTimeoutMs;
    private int connectionTimeoutMs;
    private RetryPolicy retryPolicy;
    private CuratorFramework client;

    public CuratorFactoryBean(String connectionString) {
        this(connectionString, 500, 500);
    }

    public CuratorFactoryBean(String connectionString, int sessionTimeoutMs, int connectionTimeoutMs) {
        this.connectionString = connectionString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    @Override
    public void destroy() throws Exception {
        LOGGER.info("Closing curator framework...");
        this.client.close();
        LOGGER.info("Closed curator framework.");
    }

    @Override
    public CuratorFramework getObject() throws Exception {
        return this.client;
    }

    @Override
    public Class<?> getObjectType() {
        return this.client != null ? this.client.getClass() : CuratorFramework.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (StringUtils.isEmpty(this.connectionString)) {
            throw new IllegalStateException("connectionString can not be empty.");
        } else {
            if (this.retryPolicy == null) {
                this.retryPolicy = new ExponentialBackoffRetry(1000, 2147483647, 180000);
            }

            this.client = CuratorFrameworkFactory.newClient(this.connectionString, this.sessionTimeoutMs, this.connectionTimeoutMs, this.retryPolicy);
            this.client.start();
            this.client.blockUntilConnected(30, TimeUnit.MILLISECONDS);
        }
    }
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}
