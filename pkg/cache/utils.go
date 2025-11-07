/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package cache

import (
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

func GetTaskGroupsFromAnnotation(pod *v1.Pod) ([]TaskGroup, error) {
	taskGroupInfo := utils.GetPodAnnotationValue(pod, constants.AnnotationTaskGroups)
	if taskGroupInfo == "" {
		return nil, nil
	}

	taskGroups := []TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
	}
	return taskGroups, nil
}

// RetryWithExponentialBackoff retries a function with exponential backoff.
// It performs up to maxRetries attempts with exponential backoff starting at baseDelay.
// Returns the last error if all retries fail, or nil on success.
func RetryWithExponentialBackoff(
	maxRetries int,
	baseDelay time.Duration,
	operation func() error, operationName string, taskID string, logger *zap.Logger) error {
	var lastErr error
	delay := baseDelay

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Warn("retrying operation",
				zap.String("operation", operationName),
				zap.String("taskID", taskID),
				zap.Int("attempt", attempt+1),
				zap.Int("maxRetries", maxRetries),
				zap.Duration("backoff", delay),
				zap.Error(lastErr))
			time.Sleep(delay)
			delay = delay * 2 // exponential backoff
		}

		lastErr = operation()
		if lastErr == nil {
			if attempt > 0 {
				logger.Info("operation succeeded after retry",
					zap.String("operation", operationName),
					zap.String("taskID", taskID),
					zap.Int("attempt", attempt+1))
			}
			return nil
		}
	}

	logger.Error("operation failed after all retries",
		zap.String("operation", operationName),
		zap.String("taskID", taskID),
		zap.Int("totalAttempts", maxRetries),
		zap.Error(lastErr))
	return lastErr
}
