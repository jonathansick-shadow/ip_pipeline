#!/usr/bin/env python
import math
import os

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy

import lsst.ip.isr as ipIsr

class TransformMetadataStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        #self.log = Log(self.log, "TransformMetadataStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "TransformMetadataStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Transforming metadata")
        metadata = clipboard.get(self.policy.getString("inputKeys.inputMetadata"))
        policyPath = clipboard.get(self.policy.getString("inputKeys.policyPath"))
        metadataPolicyPath = clipboard.get(self.policy.getString("inputKeys.metadataPolicyFile"))
        datatypePolicyPath = clipboard.get(self.policy.getString("inputKeys.datatypePolicyFile"))
        keywordSuffix = clipboard.get(self.policy.getString("inputKeys.keywordSuffix"))
        isValid = clipboard.get(self.policy.getString("outputKeys.isValidated"))
        metadataPolicy = pexPolicy.Policy.createPolicy(os.path.join(policyPath,metadataPolicyFile))
        datatypePolicy = pexPolicy.Policy.createPolicy(os.path.join(policyPath,datatypePolicyFile))
        ipIsr.transformMetadata(metadata, datatypePolicy, metadataPolicy,
                keywordSuffix)

        #output products
        if not isValid:
            clipboard.put(self.policy.get("outputKeys.transformedMetadata"),
                    metadata)
        
class TransformMetadataStage(harnessStage.Stage):
    parallelClass = TransformMetadataStageParallel
