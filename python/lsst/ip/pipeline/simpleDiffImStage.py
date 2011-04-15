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

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.image as afwImage

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class SimpleDiffImStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        Subtract two almost-identical Exposures

    Policy Dictionary:
        lsst/ip/pipeline/policy/simpleDiffImStageDictionary.paf

    Clipboard Input:
    - Two calibrated science Exposures

    ClipboardOutput:
    - Difference Exposure
    """
    def setup(self):
        self.log = Log(self.log, "simpleDiffImStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "SimpleDiffImStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        """
        Subtract two almost-identical Exposures
        """
        self.log.log(Log.INFO, "Differencing two Exposures in process")
        
        #grab exposure from clipboard
        exposures = []
        for k in self.policy.getArray("inputKeys.exposures"):
            exposures.append(clipboard.get(k))

        mi0 = exposures[0].getMaskedImage()
        diff = mi0.Factory(mi0, True)
        diff -= exposures[1].getMaskedImage()

        differenceExposure = afwImage.makeExposure(diff, exposures[0].getWcs())
        differenceExposure.setMetadata(exposures[0].getMetadata())
        differenceExposure.getMaskedImage().setXY0(exposures[0].getXY0())

        #output products
        clipboard.put(self.policy.get("outputKeys.differenceExposure"), differenceExposure)
        
class SimpleDiffImStage(harnessStage.Stage):
    parallelClass = SimpleDiffImStageParallel

