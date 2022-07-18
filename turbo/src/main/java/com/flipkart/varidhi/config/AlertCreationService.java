package com.flipkart.varidhi.config;

import com.flipkart.turbo.config.AlertProviderConfig;
import com.flipkart.turbo.provider.AlertServiceProvider;
import com.flipkart.turbo.utils.ZoneType;
import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.relayer.Relayer;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shah.dhruvik
 * @date 08/08/19.
 */


public class AlertCreationService {

    private static final Logger logger = LoggerFactory.getLogger(AlertCreationService.class);
    private final Map<String, AlertServiceProvider> providers = Maps.newHashMap();

    public AlertCreationService(AlertProviderConfig alertProviderConfig, String alertzEndpoint, String appName, String appID, ZoneType zone) {
        try {
            if(alertProviderConfig != null && alertProviderConfig.getAlertProviderClass() != null &&
                    !alertProviderConfig.getAlertProviderClass().isEmpty()) {
                logger.info("Loading this Alert Provider class {} " , alertProviderConfig.getAlertProviderClass());
                AlertServiceProvider provider = (AlertServiceProvider) Class.forName(alertProviderConfig.getAlertProviderClass()).newInstance();
                provider.initialize(alertzEndpoint,appName,appID,zone);
                providers.put(provider.getAlertMethodName(), provider);
            }
        } catch (Throwable e) {
            logger.warn("Error in Loading a Provider class ", e);
            throw new RuntimeException("Failed to load an Alerts provider.", e);
        }
    }

    public void createAlertsForAllRelayers(RelayerHandleContainer relayerHandleContainer, String alertMethodName) {
        AlertServiceProvider alertServiceProvider = providers.get(alertMethodName);
        if(alertServiceProvider != null) {
            logger.info("Creating Alerts for all the relayers config: {} provider: {}", alertMethodName, alertServiceProvider.getAlertMethodName());
            List<Relayer> relayers = relayerHandleContainer.getAllRelayers();
            for (Relayer relayer : relayers) {
                if (relayer.shouldCreateAlert() && relayer.isActive()) {
                    logger.info("Creating alert for the Relayer {} " , relayer.getRelayerId());
                    Map<String, String> ruleCreationStatus = alertServiceProvider.createAllAlerts(
                            relayer.getRelayerId(),
                            relayer.getRelayerConfiguration().isLeaderElectionEnabled(),
                            relayer.getRelayerConfiguration().getAlertzConfig()
                            );
                    logger.info("Alertz Rules are created/Updated. " + ruleCreationStatus);
                } else {
                    logger.info("Ignoring Alertz Creation at Startup");
                }
            }
        }
    }

    public Map<String, String> createAlertsForRelayer(Relayer relayer, String alertMethodName) {
        AlertServiceProvider alertServiceProvider = providers.get(alertMethodName);
        Map<String, String> result = new HashMap<>();
        if(alertServiceProvider != null) {
            if (relayer.shouldCreateAlert() && relayer.isActive()) {
                result = alertServiceProvider.createAllAlerts(
                        relayer.getRelayerId(),
                        relayer.getRelayerConfiguration().isLeaderElectionEnabled(),
                        relayer.getRelayerConfiguration().getAlertzConfig()
                );
                logger.info("Alertz Rules are created/Updated. " + result);
            } else {
                logger.info("Ignoring Alertz Creation at Startup");
            }
        }
        return result;
    }
}