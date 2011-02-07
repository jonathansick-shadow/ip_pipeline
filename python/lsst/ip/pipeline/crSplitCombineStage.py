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
import lsst.coadd.utils as coaddUtils

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class CrSplitCombineStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        Combine two exposures

    Policy Dictionary:
        lsst/ip/pipeline/policy/crSplitCombineStageDictionary.paf

    Clipboard Input:
    - Two calibrated science Exposures

    ClipboardOutput:
    - Difference Exposure
    """
    def setup(self):
        self.log = Log(self.log, "crSplitCombine - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "CrSplitCombineStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        """
        Combine two Exposures, omitting any pixels in either positiveDetection or negativeDetection
        """
        self.log.log(Log.INFO, "Combining two Exposures in process")
        
        #grab exposure from clipboard
        exposures = []
        for k in self.policy.getArray("inputKeys.exposures"):
            exposures.append(clipboard.get(k))
        assert len(exposures) == 2
        badPixelMask = 0x0

        mi0 = exposures[0].getMaskedImage()
        combined = mi0.Factory(mi0.getDimensions())
        combined.setXY0(mi0.getXY0())
        del mi0
        weightMap = combined.getImage().Factory(combined.getDimensions())
        weightMap.setXY0(combined.getXY0())

        for i in range(0, 2):
            e = exposures[i]
            #
            # Get the FootprintSet associated with object that are only in this Exposure
            #
            if i == 0:
                footprintKey = "positiveDetection"
            else:
                footprintKey = "negativeDetection"

            fs = clipboard.get(self.policy.get("inputKeys.%s" % footprintKey))

            mi = e.getMaskedImage()
            fs.setMask(mi.getMask(), "CR")
            fs.setMask(combined.getMask(), "CR")
            
            coaddUtils.addToCoadd(combined, weightMap, mi, mi.getMask().getPlaneBitMask("CR"), 1.0)

        combined /= weightMap

        combinedExposure = afwImage.makeExposure(combined, exposures[0].getWcs())
        combinedExposure.setMetadata(exposures[0].getMetadata())

        #output products
        clipboard.put(self.policy.get("outputKeys.combinedExposure"), combinedExposure)
        
class CrSplitCombineStage(harnessStage.Stage):
    parallelClass = CrSplitCombineStageParallel

