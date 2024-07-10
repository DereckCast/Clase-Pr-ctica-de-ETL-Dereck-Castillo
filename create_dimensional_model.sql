-- Crear Modelo Dimensional

USE DimensionalDB;


CREATE TABLE [dbo].[DimLocation] (
    LocationID BIGINT PRIMARY KEY,
    LocationAbbr VARCHAR(100),
    LocationDesc VARCHAR(255),
    Geolocation VARCHAR(255)
);

CREATE TABLE [dbo].[DimTopic] (
    TopicID VARCHAR(50) PRIMARY KEY,
    Topic VARCHAR(255)
);

CREATE TABLE [dbo].[DimQuestion] (
    QuestionID VARCHAR(50) PRIMARY KEY,
    Question VARCHAR(255)
);

CREATE TABLE [dbo].[DimStratification] (
    StratificationID1 VARCHAR(50) PRIMARY KEY,
    StratificationCategoryID1 VARCHAR(50),
    StratificationCategory1 VARCHAR(255),
    Stratification1 VARCHAR(255)
);

CREATE TABLE [dbo].[DimDataValueType] (
    DataValueTypeID VARCHAR(50) PRIMARY KEY,
    DataValueType VARCHAR(255)
);

CREATE TABLE [dbo].[FactChronicDiseases] (
    FactID BIGINT IDENTITY(1,1) PRIMARY KEY,
    YearStart BIGINT,
    YearEnd BIGINT,
    DataValue FLOAT,
    DataValueAlt FLOAT,
    LowConfidenceLimit FLOAT,
    HighConfidenceLimit FLOAT,
    DataValueUnit VARCHAR(255),
    DataValueFootnote VARCHAR(255),
    Response VARCHAR(255),
    LocationID BIGINT,
    TopicID VARCHAR(50),
    QuestionID VARCHAR(50),
    StratificationID1 VARCHAR(50),
    DataValueTypeID VARCHAR(50),
    FOREIGN KEY (LocationID) REFERENCES DimLocation(LocationID),
    FOREIGN KEY (TopicID) REFERENCES DimTopic(TopicID),
    FOREIGN KEY (QuestionID) REFERENCES DimQuestion(QuestionID),
    FOREIGN KEY (StratificationID1) REFERENCES DimStratification(StratificationID1),
    FOREIGN KEY (DataValueTypeID) REFERENCES DimDataValueType(DataValueTypeID)
);
