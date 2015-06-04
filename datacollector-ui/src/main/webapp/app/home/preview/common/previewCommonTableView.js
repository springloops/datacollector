/**
 * Controller for Preview/Snapshot Common Table View.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewCommonTableViewController', function ($scope, pipelineService, previewService) {
    var columnLimit = 5;

    angular.extend($scope, {
      inputFieldPaths: [],
      outputFieldPaths: [],
      inputLimit: columnLimit,
      outputLimit: columnLimit,

      /**
       * Return map of flatten record.
       *
       * @param record
       * @returns {{}}
       */
      getFlattenRecord: function(record) {
        if(record) {
          var flattenRecord = {};
          if(pipelineService.isCSVRecord(record.value)) {
            pipelineService.getFlattenRecordForCSVRecord(record.value, flattenRecord);
          } else {
            pipelineService.getFlattenRecord(record.value, flattenRecord);
          }
          return flattenRecord;
        }
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreInputClick: function($event) {
        $event.preventDefault();
        $scope.inputLimit += columnLimit;

        if($scope.inputLimit > $scope.inputFieldPaths.length) {
          $scope.inputLimit = $scope.inputFieldPaths.length;
        }
      },

      /**
       * Callback function when Show all link clicked.
       *
       * @param $event
       */
      onShowAllInputClick: function($event) {
        $event.preventDefault();
        $scope.inputLimit = $scope.inputFieldPaths.length;
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreOutputClick: function($event) {
        $event.preventDefault();
        $scope.outputLimit += columnLimit;
      },

      /**
       * Callback function when Show all link clicked.
       *
       * @param $event
       */
      onShowAllOutputClick: function($event) {
        $event.preventDefault();
        $scope.outputLimit = $scope.outputFieldPaths.length;

        if($scope.outputLimit > $scope.outputFieldPaths.length) {
          $scope.outputLimit = $scope.outputFieldPaths.length;
        }
      }
    });

    $scope.$watch('stagePreviewData', function(event, options) {
      updateFieldPaths();
    });

    var updateFieldPaths = function() {
      var output = $scope.stagePreviewData.output,
        input = $scope.stagePreviewData.input,
        fieldPathsList,
        fieldPaths,
        isCSVRecord,
        selectedObject = $scope.selectedObject;


      if(selectedObject.outputLanes.length > 1) {
        $scope.laneMap = previewService.getStageLaneInfo(selectedObject);
      } else {
        $scope.laneMap = null;
      }

      $scope.inputFieldPaths = [];
      if(input && input.length) {

        fieldPathsList = [];
        angular.forEach(input, function(record, index) {

          if(index === 0) {
            isCSVRecord = pipelineService.isCSVRecord(record.value);
          }

          fieldPaths = [];

          if(isCSVRecord) {
            pipelineService.getFieldPathsForCSVRecord(record.value, fieldPaths);
          } else {
            pipelineService.getFieldPaths(record.value, fieldPaths, true);
          }

          fieldPathsList.push(fieldPaths);
        });

        $scope.inputFieldPaths = _.union.apply(_, fieldPathsList);
      }

      if(columnLimit > $scope.inputFieldPaths.length) {
        $scope.inputLimit = $scope.inputFieldPaths.length;
      } else {
        $scope.inputLimit = columnLimit;
      }


      $scope.outputFieldPaths = [];
      if(output && output.length) {
        fieldPathsList = [];
        angular.forEach(output, function(record, index) {

          if(index === 0) {
            isCSVRecord = pipelineService.isCSVRecord(record.value);
          }

          fieldPaths = [];

          if(isCSVRecord) {
            pipelineService.getFieldPathsForCSVRecord(record.value, fieldPaths);
          } else {
            pipelineService.getFieldPaths(record.value, fieldPaths, true);
          }

          fieldPathsList.push(fieldPaths);
        });

        $scope.outputFieldPaths = _.union.apply(_, fieldPathsList);
      }

      if(columnLimit > $scope.outputFieldPaths.length) {
        $scope.outputLimit = $scope.outputFieldPaths.length;
      } else {
        $scope.outputLimit = columnLimit;
      }

    };


    updateFieldPaths();

  });