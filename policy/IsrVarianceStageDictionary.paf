#<?cfg paf dictionary ?>
#
# Dictionary for IsrVarianceStage policies
#
target: lsst.ip.pipeline.IsrVarianceStage

definitions: {
    #input clipboard keys
    inputKeys: {
        type: "policy"
        dictionary: {
            definitions: {
                exposure: {
                    type: "string"
                    description: "specify the clipboard key of the exposure to process."
                    default: "Exposure"
                    maxOccurs: 1
                }        
            }
        }
        maxOccurs: 1        
    }
    
    #output clipboard keys:
    outputKeys: {
        type: "policy"
        dictionary: {
            definitions: {
                varianceAddedExposure: {
                    type: "string"
                    description: "specify output clipboard key of the exposure
                    with a correct varinace plane added."
                    maxOccurs: 1
                    default: "varianceAddedExposure"
                }
            }
        }
        maxOccurs: 1
    }
}
