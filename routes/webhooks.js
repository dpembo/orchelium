module.exports = function register(app) {
app.post('/api/webhook/trigger/:jobName', asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const webhookManager = require('../webhookManager.js');
    const webhooksData = require('../webhooksData.js');
    const triggerContextModule = require('../triggerContext.js');
    
    // Extract webhook API key from request
    const providedKey = webhookManager.extractWebhookKeyFromRequest(req);
    
    if (!providedKey) {
      logger.warn(`[WEBHOOK] Trigger attempt on job [${jobName}] without API key`);
      return res.status(401).json({ error: 'Webhook API key required (via ?key= or X-Webhook-Key header)' });
    }
    
    if (!webhookManager.isValidWebhookKey(providedKey)) {
      logger.warn(`[WEBHOOK] Trigger attempt on job [${jobName}] with invalid key format`);
      return res.status(401).json({ error: 'Invalid webhook API key format' });
    }
    
    // Validate webhook key against database
    let validatedWebhook = null;
    try {
      validatedWebhook = await webhooksData.validateWebhookKey(jobName, providedKey);
      if (!validatedWebhook) {
        logger.warn(`[WEBHOOK] Webhook key not found for job [${jobName}]`);
        return res.status(401).json({ error: 'Invalid webhook API key for this job' });
      }
    } catch (validateErr) {
      logger.warn(`[WEBHOOK] Webhook key validation failed for job [${jobName}]: ${validateErr.message}`);
      return res.status(401).json({ error: 'Webhook key validation failed' });
    }
    
    const payload = req.body || {};
    
    if (!webhookManager.isValidWebhookPayload(payload)) {
      logger.warn(`[WEBHOOK] Invalid payload for job [${jobName}]`);
      return res.status(400).json({ error: 'Webhook payload must be a JSON object' });
    }

    const scheduler = require('../scheduler.js');
    const scheduledJob = scheduler.getSchedule(jobName);
    if (scheduledJob && (scheduledJob.disabled === true || scheduledJob.disabled === 'true')) {
      logger.info(`[WEBHOOK] Job [${jobName}] is disabled - trigger ignored`);
      return res.status(409).json({ success: false, error: 'Job is disabled' });
    }
    
    logger.info(`[WEBHOOK] Triggering job [${jobName}] from webhook [${validatedWebhook.id}]`);
    
    // Create webhook trigger context
    const crypto = require('crypto');
    const executionId = crypto.randomBytes(8).toString('hex');
    
    const webhookTriggerContext = await triggerContextModule.createWebhookTriggerContext(
      validatedWebhook.id,
      jobName,
      payload,
      executionId
    );
    webhookTriggerContext.webhookName = validatedWebhook.name || '';
    
    // Record the trigger event
    try {
      await webhooksData.recordWebhookTrigger(jobName, validatedWebhook.id);
    } catch (recordErr) {
      logger.warn(`[WEBHOOK] Could not record trigger event: ${recordErr.message}`);
      // Continue anyway - webhook still triggers
    }
    
    // Trigger the job asynchronously (fire & forget) - don't await it
    scheduler.runJob(jobName, 'webhook', null, webhookTriggerContext).catch(err => {
      logger.error(`[WEBHOOK] Job [${jobName}] execution failed: ${err.message}`);
    });
    
    // Return immediately with 202 Accepted + executionId before job completes
    logger.info(`[WEBHOOK] Job [${jobName}] queued for execution with executionId [${executionId}]`);
    return res.status(202).json({
      success: true,
      jobName,
      executionId,
      message: `Job triggered via webhook and queued for execution`,
      statusUrl: `/rest/orchestration/executions/${executionId}`
    });
    
  } catch (err) {
    logger.error(`[WEBHOOK] Error processing webhook trigger: ${err.message}`);
    res.status(500).json({ error: 'Webhook processing error: ' + err.message });
  }
}));

app.get('/rest/webhooks/:jobName', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const webhooksData = require('../webhooksData.js');
    
    const webhooks = await webhooksData.getWebhooksForJob(jobName);
    
    // Return webhooks without exposing API keys
    const safeWebhooks = webhooks.map(w => ({
      id: w.id,
      name: w.name,
      description: w.description,
      isActive: w.isActive,
      createdAt: w.createdAt,
      lastTriggeredAt: w.lastTriggeredAt,
      triggerCount: w.triggerCount,
      keyRotatedAt: w.keyRotatedAt
    }));
    
    res.json(safeWebhooks);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error fetching webhooks for [${req.params.jobName}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.post('/rest/webhooks/:jobName', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const { name, description } = req.body;
    const webhooksData = require('../webhooksData.js');
    
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({ error: 'Webhook name is required' });
    }
    
    const webhook = await webhooksData.createWebhook(jobName, name.trim(), description || '');
    
    logger.info(`[WEBHOOK API] Webhook created for job [${jobName}] by user`);
    
    res.status(201).json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        description: webhook.description,
        apiKey: webhook.apiKey,  // Only return key on creation
        isActive: webhook.isActive,
        createdAt: webhook.createdAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error creating webhook for [${req.params.jobName}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.put('/rest/webhooks/:jobName/:webhookId', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const { name, description, isActive } = req.body;
    const webhooksData = require('../webhooksData.js');
    
    const updates = {};
    if (name !== undefined) updates.name = name;
    if (description !== undefined) updates.description = description;
    if (isActive !== undefined) updates.isActive = isActive;
    
    const webhook = await webhooksData.updateWebhook(jobName, webhookId, updates);
    
    logger.info(`[WEBHOOK API] Webhook updated [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        description: webhook.description,
        isActive: webhook.isActive,
        createdAt: webhook.createdAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error updating webhook [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.post('/rest/webhooks/:jobName/:webhookId/rotate-key', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const { oldKey } = req.body;
    const webhooksData = require('../webhooksData.js');
    
    // For authenticated users, oldKey verification is optional
    // Since user is already in admin panel, additional verification not needed
    const webhook = await webhooksData.rotateWebhookKey(jobName, webhookId, oldKey);
    
    logger.info(`[WEBHOOK API] Webhook key rotated [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        apiKey: webhook.apiKey,  // Return new key
        keyRotatedAt: webhook.keyRotatedAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error rotating webhook key [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.delete('/rest/webhooks/:jobName/:webhookId', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const webhooksData = require('../webhooksData.js');
    
    await webhooksData.deleteWebhook(jobName, webhookId);
    
    logger.info(`[WEBHOOK API] Webhook deleted [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      message: `Webhook deleted successfully`
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error deleting webhook [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.get('/rest/webhooks-all', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  try {
    const webhooksData = require('../webhooksData.js');
    const webhooks = await webhooksData.getAllWebhooks();
    
    // Return without exposing API keys
    const safeWebhooks = webhooks.map(w => ({
      id: w.id,
      name: w.name,
      description: w.description,
      jobName: w.jobName,
      isActive: w.isActive,
      createdAt: w.createdAt,
      lastTriggeredAt: w.lastTriggeredAt,
      triggerCount: w.triggerCount,
      keyRotatedAt: w.keyRotatedAt
    }));
    
    res.json(safeWebhooks);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error fetching all webhooks:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.get('/rest/webhooks-stats', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  try {
    const webhooksData = require('../webhooksData.js');
    const stats = await webhooksData.getWebhookStats();
    res.json(stats);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error getting webhook stats:`, err);
    res.status(500).json({ error: err.message });
  }
}));
};
