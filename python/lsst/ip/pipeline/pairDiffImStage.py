#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import math

import lsst.pex.harness.stage as harnessStage
from   lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy
import lsst.pex.exceptions as pexExcept
import lsst.ip.diffim as ipDiffim

def subtractSnaps(snap1, snap2, policy, doWarping = False):
    return snapDiff
    
class PairDiffImStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage wraps image subtraction for snap pairs in one visit

    Policy Dictionary:
    lsst/ip/pipeline/policy/PairDiffImStageDictionary.paf

    Clipboard Input:
    - Snap 0 Exposure
    - Snap 1 Exposure

    Clipboard Output:
    - Difference Exposure : resulting difference image
    - Kernel Model
    - Background Model
    - Kernel Cell Set
    """
    def setup(self):
        self.log   = Log(self.log, "PairDiffImStage - parallel")
        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "PairDiffImStageDictionary.paf", "policy")
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
        self.log.log(Log.INFO, "Running snap image subtraction")
        
        # grab exposures from clipboard
        snap0Exposure = clipboard.get(self.policy.getString("inputKeys.snap0ExposureKey"))
        snap1Exposure = clipboard.get(self.policy.getString("inputKeys.snap1ExposureKey"))

        # run image subtraction
        policy = ipDiffim.makeDefaultPolicy(mergePolicy=self.diffImPolicy)
        snapPolicy = ipDiffim.modifyForSnapSubtraction(policy)
        psfmatch = ipDiffim.ImagePsfMatch(snapPolicy)
        results = psfmatch.subtractExposures(snap0Exposure, snap1Exposure,
                doWarping=True)
        snapDiff, kernelModel, bgModel, kernelCellSet = results
        
        #output products
        clipboard.put(self.policy.get("outputKeys.differenceExposureKey"),
                snapDiff)
        clipboard.put(self.policy.get("outputKeys.kernelModelKey"),
                kernelModel)
        clipboard.put(self.policy.get("outputKeys.bgModelKey"),
                bgModel)
        clipboard.put(self.policy.get("outputKeys.kernelCellSetKey"),
                kernelCellSet)

class PairDiffImStage(harnessStage.Stage):
    parallelClass = PairDiffImStageParallel
