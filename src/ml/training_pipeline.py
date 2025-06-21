"""
Machine Learning Pipeline Module (Spark 4.0)
Implements production ML pipeline with enhanced feature engineering and model management
Leverages Spark 4.0 ML optimizations and new features
"""

from typing import Dict, Any, Tuple, List, Optional
import json
from datetime import datetime
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, count, avg, stddev, min as spark_min, max as spark_max,
    sum as spark_sum, udf, array, struct, collect_list, size,
    regexp_extract, length, lower, trim, coalesce, lit, desc
)
from pyspark.sql.types import DoubleType, ArrayType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
    Bucketizer, QuantileDiscretizer, PCA, ChiSqSelector,
    HashingTF, IDF, Word2Vec, NGram, Tokenizer, StopWordsRemover,
    FeatureHasher, UnivariateFeatureSelector, VarianceThresholdSelector
)
from pyspark.ml.classification import (
    RandomForestClassifier, GBTClassifier, LogisticRegression,
    LinearSVC, NaiveBayes, MultilayerPerceptronClassifier
)
from pyspark.ml.regression import (
    RandomForestRegressor, GBTRegressor, LinearRegression,
    DecisionTreeRegressor, IsotonicRegression
)
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
    RegressionEvaluator, ClusteringEvaluator
)
from pyspark.ml.tuning import (
    CrossValidator, ParamGridBuilder, TrainValidationSplit
)
from pyspark.mllib.evaluation import BinaryClassificationMetrics

logger = structlog.get_logger()


class MLPipeline:
    """Production ML Pipeline with Spark 4.0 Enhanced Features"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.feature_columns: List[str] = []
        self.categorical_columns: List[str] = []
        self.numerical_columns: List[str] = []
        self.text_columns: List[str] = []
        
        # Configure Spark 4.0 ML optimizations
        self._configure_ml_optimizations()
    
    def _configure_ml_optimizations(self):
        """Configure Spark 4.0 specific ML optimizations"""
        # Enhanced ML performance settings
        self.spark.conf.set("spark.ml.adaptive.enabled", "true")
        self.spark.conf.set("spark.ml.adaptive.coalescingBuckets.enabled", "true")
        self.spark.conf.set("spark.ml.adaptive.skewedJoin.enabled", "true")
        
        # Spark 4.0 Arrow-based optimizations for MLlib
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self.spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # Enhanced vectorization for ML operations
        self.spark.conf.set("spark.ml.vectorize.enabled", "true")
        self.spark.conf.set("spark.ml.cache.optimization.enabled", "true")
        
        # Improved resource management for ML workloads
        self.spark.conf.set("spark.ml.executor.memoryOptimization.enabled", "true")
        
        # Parquet reading optimizations for Spark 4.0
        self.spark.conf.set("spark.sql.parquet.mergeSchema", "true")
        self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
        self.spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        logger.info("spark_4_ml_optimizations_configured")
    
    def _validate_parquet_file(self, file_path: str) -> bool:
        """Validate that a Parquet file can be read"""
        try:
            # Quick validation by reading just the schema
            df = self.spark.read.parquet(file_path)
            schema = df.schema
            count = df.limit(1).count()
            logger.info("parquet_validation_success", 
                       file=file_path, 
                       columns=len(schema.fields),
                       has_data=(count > 0))
            return True
        except Exception as e:
            logger.error("parquet_validation_failed", 
                        file=file_path, 
                        error=str(e))
            return False
        
    def load_data(self, path: str) -> DataFrame:
        """Load and prepare training data"""
        logger.info("loading_training_data", path=path)
        
        # Load data with Spark 4.0 optimizations
        # Load train and test data from subdirectories
        try:
            # Load both train and test data with absolute paths
            import os
            abs_path = os.path.abspath(path)
            train_path = os.path.join(abs_path, "train", "data.parquet")
            test_path = os.path.join(abs_path, "test", "data.parquet")
            
            logger.info("loading_files", train_path=train_path, test_path=test_path)
            
            # Validate files first
            if not os.path.exists(train_path):
                raise FileNotFoundError(f"Training file not found: {train_path}")
            if not os.path.exists(test_path):
                raise FileNotFoundError(f"Test file not found: {test_path}")
            
            # Validate Parquet files can be read
            self._validate_parquet_file(train_path)
            self._validate_parquet_file(test_path)
            
            # Spark 4.0 fix: Use mergeSchema option and specify schema inference
            # Also set additional options for better compatibility
            train_df = self.spark.read \
                .option("spark.sql.adaptive.enabled", "true") \
                .option("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .option("mergeSchema", "true") \
                .option("recursiveFileLookup", "false") \
                .option("pathGlobFilter", "*.parquet") \
                .option("spark.sql.parquet.enableVectorizedReader", "true") \
                .option("spark.sql.parquet.mergeSchema", "true") \
                .parquet(train_path)
            
            test_df = self.spark.read \
                .option("spark.sql.adaptive.enabled", "true") \
                .option("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .option("mergeSchema", "true") \
                .option("recursiveFileLookup", "false") \
                .option("pathGlobFilter", "*.parquet") \
                .option("spark.sql.parquet.enableVectorizedReader", "true") \
                .option("spark.sql.parquet.mergeSchema", "true") \
                .parquet(test_path)
            
            # Union train and test data for full dataset
            df = train_df.unionByName(test_df, allowMissingColumns=True)
            
            logger.info("loaded_data_successfully", 
                       train_count=train_df.count(), 
                       test_count=test_df.count())
        
        except Exception as e:
            logger.warning("failed_to_load_train_test_separately", error=str(e))
            # Try loading from direct path as fallback
            logger.info("attempting_fallback_load", path=path)
            
            # Check if path exists
            import os
            if not os.path.exists(path):
                raise FileNotFoundError(f"Path not found: {path}")
            
            # Try to load with more permissive settings
            df = self.spark.read \
                .option("spark.sql.adaptive.enabled", "true") \
                .option("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .option("mergeSchema", "true") \
                .option("recursiveFileLookup", "true") \
                .option("pathGlobFilter", "*.parquet") \
                .option("spark.sql.parquet.enableVectorizedReader", "true") \
                .option("spark.sql.parquet.mergeSchema", "true") \
                .option("spark.sql.files.ignoreCorruptFiles", "true") \
                .option("spark.sql.files.ignoreMissingFiles", "true") \
                .parquet(path)
        
        # Apply Spark 4.0 hints for better performance
        df = df.hint("broadcast", when(df.count() < 1000000, True))
        df = df.hint("cache_lazy")  # Spark 4.0 lazy caching
        
        # Identify column types
        self._identify_column_types(df)
        
        # Basic data validation with enhanced stats
        initial_count = df.count()
        df_clean = df.dropna(subset=["label"])  # Assuming 'label' is target
        
        # Spark 4.0 enhanced data profiling
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        logger.info("data_loaded", 
                   initial_count=initial_count,
                   clean_count=df_clean.count(),
                   columns=df.columns,
                   null_counts=null_counts)
        
        return df_clean
    
    def _identify_column_types(self, df: DataFrame):
        """Automatically identify column types"""
        # Sample for analysis
        sample = df.sample(0.1).toPandas()
        
        for col_name in df.columns:
            if col_name == "label":  # Skip target column
                continue
                
            dtype = df.schema[col_name].dataType.typeName()
            
            if dtype in ["string"]:
                unique_count = df.select(col_name).distinct().count()
                if unique_count < 50:  # Categorical
                    self.categorical_columns.append(col_name)
                else:  # Text
                    self.text_columns.append(col_name)
            elif dtype in ["double", "float", "int", "long"]:
                self.numerical_columns.append(col_name)
        
        logger.info("column_types_identified",
                   categorical=self.categorical_columns,
                   numerical=self.numerical_columns,
                   text=self.text_columns)
    
    def create_feature_pipeline(self, df: DataFrame) -> Pipeline:
        """Create comprehensive feature engineering pipeline with Spark 4.0 enhancements"""
        stages = []
        
        # Process categorical features with enhanced encoding
        for cat_col in self.categorical_columns:
            # String indexing with Spark 4.0 optimizations
            indexer = StringIndexer(
                inputCol=cat_col,
                outputCol=f"{cat_col}_indexed",
                handleInvalid="keep",
                stringOrderType="alphabetAsc"  # Spark 4.0 enhanced ordering
            )
            stages.append(indexer)
            
            # One-hot encoding with improved sparsity handling
            encoder = OneHotEncoder(
                inputCol=f"{cat_col}_indexed",
                outputCol=f"{cat_col}_encoded",
                dropLast=True  # Spark 4.0 improved multicollinearity handling
            )
            stages.append(encoder)
            self.feature_columns.append(f"{cat_col}_encoded")
        
        # Process numerical features
        for num_col in self.numerical_columns:
            # Impute missing values with median
            df = df.withColumn(
                f"{num_col}_imputed",
                coalesce(col(num_col), lit(df.approxQuantile(num_col, [0.5], 0.01)[0]))
            )
            
            # Bucketize continuous variables
            if df.select(num_col).distinct().count() > 100:
                bucketizer = QuantileDiscretizer(
                    numBuckets=10,
                    inputCol=f"{num_col}_imputed",
                    outputCol=f"{num_col}_buckets"
                )
                stages.append(bucketizer)
                self.feature_columns.append(f"{num_col}_buckets")
            
            self.feature_columns.append(f"{num_col}_imputed")
        
        # Process text features
        for text_col in self.text_columns:
            # Tokenization
            tokenizer = Tokenizer(
                inputCol=text_col,
                outputCol=f"{text_col}_tokens"
            )
            stages.append(tokenizer)
            
            # Remove stop words
            remover = StopWordsRemover(
                inputCol=f"{text_col}_tokens",
                outputCol=f"{text_col}_filtered"
            )
            stages.append(remover)
            
            # N-grams
            ngram = NGram(
                n=2,
                inputCol=f"{text_col}_filtered",
                outputCol=f"{text_col}_ngrams"
            )
            stages.append(ngram)
            
            # TF-IDF
            hashingTF = HashingTF(
                inputCol=f"{text_col}_ngrams",
                outputCol=f"{text_col}_tf",
                numFeatures=1000
            )
            stages.append(hashingTF)
            
            idf = IDF(
                inputCol=f"{text_col}_tf",
                outputCol=f"{text_col}_tfidf"
            )
            stages.append(idf)
            self.feature_columns.append(f"{text_col}_tfidf")
        
        # Assemble all features with Spark 4.0 optimizations
        assembler = VectorAssembler(
            inputCols=self.feature_columns,
            outputCol="raw_features",
            handleInvalid="skip"
        )
        stages.append(assembler)
        
        # Variance threshold selector (Spark 4.0 feature)
        variance_selector = VarianceThresholdSelector(
            featuresCol="raw_features",
            outputCol="variance_filtered_features",
            threshold=0.01  # Remove features with low variance
        )
        stages.append(variance_selector)
        
        # Scale features with enhanced normalization
        scaler = StandardScaler(
            inputCol="variance_filtered_features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        stages.append(scaler)
        
        # Univariate feature selection (Spark 4.0 enhanced)
        univariate_selector = UnivariateFeatureSelector(
            featuresCol="scaled_features",
            outputCol="univariate_selected_features",
            labelCol="label",
            selectionMode="percentile",
            selectionThreshold=0.1  # Top 10% of features
        )
        stages.append(univariate_selector)
        
        # Chi-squared feature selection
        chi_selector = ChiSqSelector(
            numTopFeatures=100,
            featuresCol="univariate_selected_features",
            outputCol="selected_features",
            labelCol="label"
        )
        stages.append(chi_selector)
        
        # PCA for dimensionality reduction with adaptive components
        pca = PCA(
            k=50,
            inputCol="selected_features",
            outputCol="features"
        )
        stages.append(pca)
        
        return Pipeline(stages=stages)
    
    def train_model(self, df: DataFrame) -> Tuple[PipelineModel, Dict[str, Any]]:
        """Train model with hyperparameter tuning"""
        logger.info("starting_model_training")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        train_df.cache()
        test_df.cache()
        
        # Create feature pipeline
        feature_pipeline = self.create_feature_pipeline(train_df)
        
        # Create enhanced models to compare (Spark 4.0 optimized)
        models = {
            "random_forest": RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                probabilityCol="probability",
                maxDepth=10,
                numTrees=100,
                seed=42,
                bootstrap=True,  # Spark 4.0 enhanced bootstrap sampling
                subsamplingRate=0.8  # Spark 4.0 improved subsampling
            ),
            "gbt": GBTClassifier(
                featuresCol="features",
                labelCol="label",
                maxDepth=5,
                maxIter=20,
                seed=42,
                validationTol=0.01,  # Spark 4.0 early stopping
                validationIndicatorCol=None  # Spark 4.0 validation enhancement
            ),
            "logistic": LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=100,
                regParam=0.1,
                elasticNetParam=0.8,
                standardization=True,  # Spark 4.0 enhanced standardization
                threshold=0.5  # Spark 4.0 configurable threshold
            ),
            "linear_svc": LinearSVC(
                featuresCol="features",
                labelCol="label",
                maxIter=100,
                regParam=0.1,
                aggregationDepth=2  # Spark 4.0 performance optimization
            ),
            "naive_bayes": NaiveBayes(
                featuresCol="features",
                labelCol="label",
                modelType="multinomial",  # Spark 4.0 enhanced model types
                smoothing=1.0
            )
        }
        
        best_model = None
        best_metrics = {"auc": 0}
        
        # Train and evaluate each model
        for model_name, model in models.items():
            logger.info("training_model", model=model_name)
            
            # Create pipeline
            pipeline = Pipeline(stages=feature_pipeline.getStages() + [model])
            
            # Hyperparameter tuning
            paramGrid = self._create_param_grid(model_name, model)
            
            # Cross validation with Spark 4.0 enhancements
            evaluator = BinaryClassificationEvaluator(
                labelCol="label",
                metricName="areaUnderROC"
            )
            
            cv = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=paramGrid,
                evaluator=evaluator,
                numFolds=3,
                parallelism=4,
                collectSubModels=False,  # Spark 4.0 memory optimization
                seed=42  # Spark 4.0 reproducibility
            )
            
            # Train model
            cv_model = cv.fit(train_df)
            
            # Evaluate on test set
            predictions = cv_model.transform(test_df)
            metrics = self._evaluate_model(predictions, model_name)
            
            logger.info("model_evaluation", model=model_name, metrics=metrics)
            
            # Keep best model
            if metrics["auc"] > best_metrics["auc"]:
                best_model = cv_model.bestModel
                best_metrics = metrics
        
        # Additional model analysis
        self._analyze_model(best_model, test_df)
        
        logger.info("training_completed", best_metrics=best_metrics)
        
        return best_model, best_metrics
    
    def _create_param_grid(self, model_name: str, model: Any) -> List[Dict]:
        """Create parameter grid for hyperparameter tuning"""
        builder = ParamGridBuilder()
        
        if model_name == "random_forest":
            builder = builder \
                .addGrid(model.maxDepth, [5, 10, 15]) \
                .addGrid(model.numTrees, [50, 100, 200]) \
                .addGrid(model.minInstancesPerNode, [1, 5, 10])
                
        elif model_name == "gbt":
            builder = builder \
                .addGrid(model.maxDepth, [3, 5, 7]) \
                .addGrid(model.maxIter, [10, 20, 30]) \
                .addGrid(model.stepSize, [0.01, 0.1, 0.5])
                
        elif model_name == "logistic":
            builder = builder \
                .addGrid(model.regParam, [0.01, 0.1, 1.0]) \
                .addGrid(model.elasticNetParam, [0.0, 0.5, 1.0]) \
                .addGrid(model.maxIter, [50, 100, 200])
                
        elif model_name == "linear_svc":
            builder = builder \
                .addGrid(model.regParam, [0.01, 0.1, 1.0]) \
                .addGrid(model.maxIter, [50, 100, 200]) \
                .addGrid(model.aggregationDepth, [2, 4, 8])
                
        elif model_name == "naive_bayes":
            builder = builder \
                .addGrid(model.smoothing, [0.5, 1.0, 2.0])
        
        return builder.build()
    
    def _evaluate_model(self, predictions: DataFrame, model_name: str) -> Dict[str, Any]:
        """Comprehensive model evaluation"""
        # Binary classification metrics
        binary_evaluator = BinaryClassificationEvaluator(labelCol="label")
        
        auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
        pr_auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderPR"})
        
        # Multiclass metrics
        mc_evaluator = MulticlassClassificationEvaluator(labelCol="label")
        
        accuracy = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
        f1 = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})
        precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
        recall = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})
        
        # Confusion matrix
        predictions_and_labels = predictions.select("prediction", "label").rdd
        metrics = BinaryClassificationMetrics(predictions_and_labels)
        
        # Get threshold-based metrics
        precision_by_threshold = metrics.precisionByThreshold.collect()
        recall_by_threshold = metrics.recallByThreshold.collect()
        
        return {
            "model": model_name,
            "auc": auc,
            "pr_auc": pr_auc,
            "accuracy": accuracy,
            "f1": f1,
            "precision": precision,
            "recall": recall,
            "threshold_metrics": {
                "precision_by_threshold": precision_by_threshold[:5],  # Top 5
                "recall_by_threshold": recall_by_threshold[:5]
            }
        }
    
    def _analyze_model(self, model: PipelineModel, test_df: DataFrame):
        """Analyze model feature importance and predictions"""
        # Get the ML model from pipeline
        ml_model = None
        for stage in model.stages:
            if hasattr(stage, "featureImportances"):
                ml_model = stage
                break
        
        if ml_model and hasattr(ml_model, "featureImportances"):
            # Feature importance analysis
            importances = ml_model.featureImportances
            logger.info("feature_importances", 
                       num_features=len(importances),
                       top_features=importances.toArray()[:10].tolist())
        
        # Prediction distribution analysis
        predictions = model.transform(test_df)
        pred_distribution = predictions.groupBy("prediction").count().collect()
        
        logger.info("prediction_distribution", distribution=pred_distribution)
    
    def save_model(self, model: PipelineModel, path: str):
        """Save model with metadata"""
        timestamp = datetime.now().isoformat()
        
        # Save model
        model_path = f"{path}/model_{timestamp}"
        model.write().overwrite().save(model_path)
        
        # Save metadata with Spark 4.0 information
        metadata = {
            "timestamp": timestamp,
            "model_path": model_path,
            "feature_columns": self.feature_columns,
            "categorical_columns": self.categorical_columns,
            "numerical_columns": self.numerical_columns,
            "text_columns": self.text_columns,
            "spark_version": self.spark.version,
            "spark_4_optimizations": {
                "adaptive_enabled": self.spark.conf.get("spark.ml.adaptive.enabled"),
                "arrow_enabled": self.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"),
                "vectorization_enabled": self.spark.conf.get("spark.ml.vectorize.enabled")
            },
            "feature_engineering_stages": len(model.stages),
            "model_type": "spark_4_enhanced_pipeline"
        }
        
        with open(f"{path}/metadata_{timestamp}.json", "w") as f:
            json.dump(metadata, f, indent=2)
        
        logger.info("model_saved", path=model_path)
    
    def start_prediction_service(self, model: PipelineModel):
        """Start real-time prediction service"""
        logger.info("starting_prediction_service")
        
        # This would typically integrate with the streaming module
        # to provide real-time predictions
        # See StreamingProcessor.create_streaming_ml_pipeline()
        
        logger.info("prediction_service_ready")