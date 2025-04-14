import sys,os
from Network_security.exception.exception import NetworkSecurityException
from Network_security.logging.logger import logging
from Network_security.components.data_ingestion import DataIngestion
from Network_security.components.data_validation import DataValidation
from Network_security.components.data_transformation import DataTransformation
from Network_security.components.model_trainer import ModelTrainer
from Network_security.entity.config_entity import(
TrainingPipelineConfig,DataIngestionConfig,DataValidationConfig,DataTransformationConfig,ModelTrainerConfig
)
from Network_security.entity.artifact_entity import(
DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,ModelTrainerArtifact
) 

class TrainingPipeline:
    def __init__(self):
        self.training_pipeline_config=TrainingPipelineConfig()

    def start_data_ingestion(self):
        try:
            self.data_ingestion_config=DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("Initiate data ingestion")
            data_ingestion=DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact=data_ingestion.initiate_data_ingestion()
            logging.info(f"Data ingestion completed and artifact:{data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def start_data_validation(self,data_ingestion_artifact:DataIngestionArtifact):
        try:
            data_validation_config=DataValidationConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("Initiate data validation")
            data_validation=DataValidation(data_ingestion_artifact=data_ingestion_artifact,
                                           data_validation_config=data_validation_config)
            dat_validation_artifact=data_validation.initiate_data_validation()
            logging.info(f"Data validation completed and artifact:{dat_validation_artifact}")
            return dat_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def start_data_transforamtion(self,data_validation_artifact:DataValidationArtifact):
        try:
            data_transformation_config=DataTransformationConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("Initiate data transformation")
            data_transformation=DataTransformation(data_transforamtion_config=data_transformation_config,
                                                data_validation_artifact=data_validation_artifact)
            data_transformation_artifact=data_transformation.initiate_data_transformation()
            logging.info(f"Data transformation completed and artifact:{data_transformation_artifact}")
            return data_transformation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def start_model_trainer(self,data_transformation_artifact=DataTransformationArtifact):
        try:
            model_trainer_config=ModelTrainerConfig(training_pipleline_config=self.training_pipeline_config)
            logging.info("Initiate model trainer")
            model_trainer=ModelTrainer(model_trainer_config=model_trainer_config,
                                    data_transforamtion_artifact=data_transformation_artifact)
            model_trainer_artifact=model_trainer.initiate_model_trainer()
            logging.info(f"Model trainer completed and artifact{model_trainer_artifact}")
            return model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def run_pipeline(self):
        try:
            data_ingestion_artifact=self.start_data_ingestion()
            data_validation_artifact=self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact=self.start_data_transforamtion(data_validation_artifact=data_validation_artifact)
            model_trainer_artifact=self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            return model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)










if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion=DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data Initiation Completed")
        print(dataingestionartifact)

        data_validation_config=DataValidationConfig(trainingpipelineconfig)
        data_validation=DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate the data Validation")
        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("data Validation Completed")
        print(data_validation_artifact)

        data_transformation_config=DataTransformationConfig(trainingpipelineconfig)
        data_transformation=DataTransformation(data_validation_artifact,data_transformation_config)
        logging.info("Initiate the Data Tranformation")
        data_transformation_artifact=data_transformation.initiate_data_transformation()
        logging.info("data Transformation Completed")
        print(data_transformation_artifact)

        logging.info("Model Training started")
        model_trainer_config=ModelTrainerConfig(trainingpipelineconfig)
        model_trainer=ModelTrainer(model_trainer_config=model_trainer_config,data_transforamtion_artifact=data_transformation_artifact)
        model_trainer_artifact=model_trainer.initiate_model_trainer()
        logging.info("Model Training Artifact Created")
   
    except Exception as e:
        raise NetworkSecurityException(e,sys)