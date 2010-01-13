#!/usr/bin/env python
import math
import os

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy

import lsst.ip.isr as ipIsr

class ValidateMetadataStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        #self.log = Log(self.log, "ValidateMetadataStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "TransformMetadataStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Vaidating metadata")
        metadata = clipboard.get(self.policy.getString("outputKeys.transformedMetadata"))
        policyPath = clipboard.get(self.policy.getString("inputKeys.policyPath"))
        metadataPolicyFile = clipboard.get(self.policy.getString("inputKeys.metadataPolicyFile"))
        metadataPolicy = pexPolicy.Policy.createPolicy(os.path.join(policyPath,metadataPolicyFile))
        #grab exposure from clipboard
        isValid = ipIsr.validateMetadata(metadata, metadataPolicy)

        #output products
        clipboard.put(self.policy.get("outputKeys.isValidated", isValid),
                metadata)
        
class ValidateMetadataStage(harnessStage.Stage):
    parallelClass = ValidateMetadataStageParallel
