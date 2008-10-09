class BdrbJobQueue < ActiveRecord::Base
  validates_uniqueness_of :job_key,:scope => [:worker_name,:worker_key]
  def self.find_next(worker_name,worker_key = nil)
    returned_job = nil
    transaction do
      unless worker_key
        #use ruby time stamps for time calculations as db might have different times than what is calculated by ruby/rails
        t_job_id = find(:first, :conditions => [" worker_name = ? AND taken = ? AND scheduled_at <= ? ", worker_name, 0, Time.now.utc], :select => 'id')
      else
        t_job_id = find(:first, :conditions => [" worker_name = ? AND taken = ? AND worker_key = ? AND scheduled_at <= ? ", worker_name, 0, worker_key, Time.now.utc ], :select => 'id')
      end
      if t_job_id
        t_job = find(t_job_id[:id], :lock => true)
        t_job.taken = 1
        t_job.started_at = Time.now
        t_job.save
        returned_job = t_job
      end
    end
    returned_job
  end

  def release_job
    self.class.transaction do
      self.taken = 0
      self.started_at = nil
      self.save
    end
  end

  def self.insert_job(options = { })
    transaction do
      options.merge!(:submitted_at => Time.now,:finished => 0,:taken => 0)
      t_job = new(options)
      t_job.save
    end
  end

  def self.remove_job(options = { })
    transaction do
      t_id = find(:first, :conditions => options.merge(:finished => 0,:taken => 0), :select => 'id')
      if t_id
        t_job_id = find(t_id[:id], :lock => true)
        delete(t_job_id)
      end
    end
  end

  def finish!
    self.class.transaction do
      self.finished = 1
      self.finished_at = Time.now
      self.job_key = "finished_#{Time.now.to_i}_#{job_key}"
      self.save
    end
    Thread.current[:persistent_job_id] = nil
    Thread.current[:job_key] = nil
    nil
  end
end

