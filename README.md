# selbat-scalable
<img src="https://github.com/roby-avo/selbat-scalable/assets/64791054/c1f091fd-c382-4d56-acd7-00ca6fc434f1" width="200" height="200">

selBat provides a process that performs EL on tables. Given a KG containing a set of entities 𝐸 and a collection of named-entity mentions 𝑀, the goal of EL is to map each entity mention 𝑚 ∈ 𝑀 to its corresponding entity 𝑒 ∈ 𝐸 in the KG (CEA task). Moreover, given the subject column S, the approach provides the annotation, between S-column and other columns in the table, with a predicate in the KG (CPA task). Eventually, the approach provides the annotation for each column with a concept in the KG (CTA task). 

Selbat has been implemented to allow pipeline execution for large data processing. In particular, as you can see in the figure below, the pipeline consists of 7 phases, and each phase is implemented as an independent Docker container. The phases are as follows:In paritcular, as you can see in the figure below the pipeline consinsts in 7 phases and each phase is implemented as independet docker container, the phases are:
1) Data analysis and pre-processing: This phase involves data normalization and identification of the different data types of the columns in the table using LamAPI, which is an external tool
2) Lookup: For each entity, entity retrieval is performed using LamAPI, which contains data from KG (Knowledge Graph)
3) Feature Extraction: For each candidate, the features are computed
4) Predictions: Predictions: The ML model makes predictions to obtain the ranking of the candidates
5) Features Extraction Revision: Result aggregation is performed considering columns' consistency information, such as types and predicates
6) Predictions: The ML model makes predictions again to obtain the final ranking of the candidates, considering type and predicate consistency


Selbat pipeline
![pipeline_v3](https://github.com/roby-avo/selbat-scalable/assets/64791054/f322339b-6caa-4d7a-a710-681f2409c04b)
