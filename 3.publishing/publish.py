"""
White lable publishing module.

Depending on input, can publish to different topics if we go that route. 

Batch processess protobuf messages to a single partition to ensure ordering.
"""