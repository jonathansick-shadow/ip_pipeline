#!/usr/bin/env python
import math

import lsst.pex.harness.stage as harnessStage
from   lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy
import lsst.pex.exceptions as pexExcept
import lsst.ip.diffim as ipDiffim

class DiffImStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage wraps image subtraction        

    Policy Dictionary:
    lsst/ip/pipeline/policy/DiffImStageDictionary.paf

    Clipboard Input:
    - Template Exposure : to be convolved
    - Science Exposure  : to be matched to

    Clipboard Output:
    - Difference Exposure : resulting difference image
    - Psf Matching Kernel : the spatial model of the Psf matching Kernel
    - Background Function : differential background model
    """
    def setup(self):
        self.log   = Log(self.log, "DiffImStage - parallel")
        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                                 "DiffImStageDictionary.paf", "policy")
        defPolicy  = pexPolicy.Policy.createPolicy(policyFile,
                                                   policyFile.getRepositoryPath(), # repos
                                                   True)                           # validate

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())
        self.diffImPolicy = self.policy.get("diffImPolicy")

    def process(self, clipboard):
        """
        Run image subtraction
        """
        self.log.log(Log.INFO, "Running image subtraction")
        
        # grab exposures from clipboard
        templateExposure = clipboard.get(self.policy.getString("inputKeys.templateExposureKey"))
        scienceExposure  = clipboard.get(self.policy.getString("inputKeys.scienceExposureKey"))

        # run image subtraction
        results = ipDiffim.subtractExposure(templateExposure,
                                            scienceExposure,
                                            self.diffImPolicy)
        
        # parse results
        differenceExposure, spatialKernel, backgroundModel, kernelCellSet = results

        #output products
        clipboard.put(self.policy.get("outputKeys.differenceExposureKey"), differenceExposure)
        clipboard.put(self.policy.get("outputKeys.psfMatchingKernelKey"), spatialKernel)
        clipboard.put(self.policy.get("outputKeys.backgroundFunctionKey"), backgroundModel)
        
class DiffImStage(harnessStage.Stage):
    parallelClass = DiffImStageParallel

