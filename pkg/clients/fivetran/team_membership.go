package fivetran

import (
	"context"
	"fmt"
	"sync"

	"github.com/redhat-data-and-ai/usernaut/pkg/common/structs"
	"github.com/redhat-data-and-ai/usernaut/pkg/logger"
	"github.com/sirupsen/logrus"
)

func (fc *FivetranClient) FetchTeamMembersByTeamID(
	ctx context.Context,
	teamID string) (map[string]*structs.User, error) {

	log := logger.Logger(ctx).WithFields(logrus.Fields{
		"service": "fivetran",
		"teamID":  teamID,
	})
	log.Info("fetching team members by team ID")

	teamMembers := make(map[string]*structs.User, 0)
	resp, err := fc.fivetranClient.NewTeamUserMembershipsList().
		TeamId(teamID).
		Do(ctx)
	if err != nil {
		log.WithError(err).Error("error fetching team members by team ID")
		return nil, err
	}
	for _, item := range resp.Data.Items {
		teamMembers[item.UserId] = &structs.User{
			ID:   item.UserId,
			Role: item.Role,
		}
	}

	cursor := resp.Data.NextCursor
	for len(cursor) != 0 {
		resp, err := fc.fivetranClient.NewTeamUserMembershipsList().
			TeamId(teamID).
			Cursor(cursor).
			Do(ctx)
		if err != nil {
			log.WithField("response", resp.Code).WithError(err).Error("error fetching list of team members")
			return nil, err
		}
		for _, item := range resp.Data.Items {
			teamMembers[item.UserId] = &structs.User{
				ID:   item.UserId,
				Role: item.Role,
			}
		}
		cursor = resp.Data.NextCursor
	}

	return teamMembers, nil

}

const MaxConcurrent = 10 //we can change it accfordingly

const maxConcurrentUsers = 10

func (fc *FivetranClient) AddUserToTeam(ctx context.Context, teamID string, userIDs []string) error {
	log := logger.Logger(ctx).WithFields(logrus.Fields{
		"service":    "fivetran",
		"teamID":     teamID,
		"user_count": len(userIDs),
	})

	log.Info("adding userds to the team")

	var wg sync.WaitGroup
	errr := make(chan error, len(userIDs)) // this is an errror channel
	sem := make(chan struct{}, maxConcurrentUsers)

	for _, id := range userIDs {
		wg.Add(1)
		sem <- struct{}{}

		go func(uid string) {
			defer wg.Done()
			defer func() { <-sem }()
			log := logger.Logger(ctx).WithFields(logrus.Fields{
				"service": "fivetran",
				"teamID":  teamID,
				"userID":  uid,
			})

			log.Info("adding user to fivetran team ")
			resp, err := fc.fivetranClient.
				NewTeamUserMembershipCreate().
				TeamId(teamID).
				UserId(uid).
				Role("Team Member").
				Do(ctx)

			if err != nil {
				log.WithField("response", resp.CommonResponse).WithError(err).
					Error("Error adding user to team")
				errr <- fmt.Errorf("%s: %w", uid, err)
				return
			}
			log.Info("added userds to the team successfuly")
		}(id)
	}

	wg.Wait()
	close(errr)

	if err, ok := <-errr; ok {
		return err
	}
	return nil
}

func (fc *FivetranClient) RemoveUserFromTeam(ctx context.Context, teamID string, userIDs []string) error {
	log := logger.Logger(ctx).WithFields(logrus.Fields{
		"service":    "fivetran",
		"teamID":     teamID,
		"user_count": len(userIDs),
	})

	log.Info("removing user from the team")
	var wg sync.WaitGroup
	errr := make(chan error, len(userIDs))
	sem := make(chan struct{}, maxConcurrentUsers)

	for _, id := range userIDs {
		wg.Add(1)
		sem <- struct{}{}

		go func(uid string) {
			defer wg.Done()
			defer func() { <-sem }()
			log := logger.Logger(ctx).WithFields(logrus.Fields{
				"service": "fivetran",
				"teamID":  teamID,
				"userID":  uid,
			})
			log.Info("removing user from the team")
			resp, err := fc.fivetranClient.NewTeamUserMembershipDelete().
				TeamId(teamID).
				UserId(uid).
				Do(ctx)
			if err != nil {
				log.WithField("response", resp).WithError(err).Error("error removing user from the team")
				return

			}
			log.Info("users removed from team successfuly")
		}(id)

	}

	wg.Wait()
	close(errr)

	if err, ok := <-errr; ok {
		return err
	}
	return nil
}
